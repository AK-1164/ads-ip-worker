export default {
  async fetch(request, env, ctx) {
    return handleFetch(request, env, ctx);
  },

  async scheduled(event, env, ctx) {
    ctx.waitUntil(runRetryQueue(env, 20));
  },
};

async function handleFetch(request, env, ctx) {
  if (request.method !== "POST") {
    return new Response("Method Not Allowed", { status: 405 });
  }

  const secret = request.headers.get("x-worker-secret");
  if (!secret || secret !== env.WORKER_SHARED_SECRET) {
    console.log("Unauthorized request: bad or missing secret");
    return new Response("Unauthorized", { status: 401 });
  }

  let body;
  try {
    body = await request.json();
  } catch (err) {
    console.log("Invalid JSON body", String(err));
    return new Response("Invalid JSON", { status: 400 });
  }

  const ip = String(body?.ip || "").trim();
  const country = String(body?.country || "XX").trim();
  const ts = Number(body?.ts || Math.floor(Date.now() / 1000));

  if (!ip) {
    return new Response("Missing IP", { status: 400 });
  }

  if (!isValidIp(ip)) {
    console.log("Invalid IP format", ip);
    await queueFailure(env, ip, country, ts, "invalid ip format");
    return new Response("Invalid IP", { status: 400 });
  }

  console.log("Incoming exclusion request", { ip, country, ts });

  const result = await processCurrentIp(env, ip, country, ts);

  ctx.waitUntil(runRetryQueue(env, 10, ip));

  return new Response(result.message, { status: result.status });
}

async function processCurrentIp(env, ip, country, ts) {
  try {
    const existing = await env.DB
      .prepare("SELECT ip, resource_name FROM ads_exclusions WHERE ip = ?")
      .bind(ip)
      .first();

    if (existing) {
      await safeUpdateIpLog(env, ip, 1, "already_excluded");
      await env.DB.prepare("DELETE FROM exclusion_queue WHERE ip = ?").bind(ip).run();
      console.log("Already excluded:", ip);
      return { status: 200, message: "Already excluded" };
    }

    const accessToken = await getAccessToken(env);
    const resourceName = await addIpBlock(env, accessToken, ip);

    await env.DB.prepare(`
      INSERT INTO ads_exclusions (ip, resource_name, created_at)
      VALUES (?, ?, ?)
      ON CONFLICT(ip) DO UPDATE SET
        resource_name = excluded.resource_name,
        created_at = excluded.created_at
    `).bind(ip, resourceName, ts).run();

    await safeUpdateIpLog(env, ip, 1, "success");
    await env.DB.prepare("DELETE FROM exclusion_queue WHERE ip = ?").bind(ip).run();

    console.log("Direct add success", { ip, resourceName });
    return { status: 200, message: "OK" };
  } catch (err) {
    const msg = normalizeError(err);
    console.log("Direct worker exception", { ip, error: msg });

    try {
      await queueFailure(env, ip, country, ts, msg);
    } catch (queueErr) {
      console.log("queueFailure failed", {
        ip,
        originalError: msg,
        queueError: normalizeError(queueErr),
      });
      await safeUpdateIpLog(env, ip, 0, `queue_failure: ${msg.slice(0, 200)}`);
    }

    return { status: 202, message: "Queued" };
  }
}

async function runRetryQueue(env, limit = 10, skipIp = null) {
  let rows;
  try {
    rows = await env.DB.prepare(`
      SELECT ip, country, created_at, attempts
      FROM exclusion_queue
      WHERE processed = 0
      ORDER BY attempts ASC, created_at ASC
      LIMIT ?
    `).bind(limit).all();
  } catch (err) {
    console.log("Failed to read exclusion_queue", normalizeError(err));
    return;
  }

  if (!rows?.results?.length) {
    console.log("Retry queue empty");
    return;
  }

  let accessToken;
  try {
    accessToken = await getAccessToken(env);
  } catch (err) {
    console.log("Retry queue token fetch failed", normalizeError(err));
    return;
  }

  for (const row of rows.results) {
    const ip = row.ip;
    if (!ip || ip === skipIp) continue;

    const country = row.country || "XX";
    const ts = Number(row.created_at || Math.floor(Date.now() / 1000));

    try {
      const existing = await env.DB
        .prepare("SELECT ip FROM ads_exclusions WHERE ip = ?")
        .bind(ip)
        .first();

      if (existing) {
        await env.DB.prepare(`
          UPDATE exclusion_queue
          SET processed = 1, last_error = NULL
          WHERE ip = ?
        `).bind(ip).run();

        await safeUpdateIpLog(env, ip, 1, "success");
        console.log("Retry skipped, already excluded", { ip });
        continue;
      }

      const resourceName = await addIpBlock(env, accessToken, ip);

      await env.DB.prepare(`
        INSERT INTO ads_exclusions (ip, resource_name, created_at)
        VALUES (?, ?, ?)
        ON CONFLICT(ip) DO UPDATE SET
          resource_name = excluded.resource_name,
          created_at = excluded.created_at
      `).bind(ip, resourceName, ts).run();

      await env.DB.prepare(`
        UPDATE exclusion_queue
        SET processed = 1, last_error = NULL
        WHERE ip = ?
      `).bind(ip).run();

      await safeUpdateIpLog(env, ip, 1, "success");
      console.log("Retry add success", { ip, resourceName });
    } catch (err) {
      const msg = normalizeError(err);
      console.log("Retry exception", { ip, error: msg });

      try {
        await queueFailure(env, ip, country, ts, msg);
      } catch (queueErr) {
        console.log("Retry queueFailure failed", {
          ip,
          originalError: msg,
          queueError: normalizeError(queueErr),
        });
        await safeUpdateIpLog(env, ip, 0, `retry_queue_failure: ${msg.slice(0, 200)}`);
      }
    }
  }
}

async function queueFailure(env, ip, country, ts, errorText) {
  const errMsg = String(errorText || "unknown error").slice(0, 1000);

  console.log("queueFailure", { ip, country, errMsg });

  await env.DB.prepare(`
    INSERT INTO exclusion_queue (ip, country, created_at, attempts, last_error, processed)
    VALUES (?, ?, ?, 1, ?, 0)
    ON CONFLICT(ip) DO UPDATE SET
      country = excluded.country,
      created_at = exclusion_queue.created_at,
      attempts = COALESCE(exclusion_queue.attempts, 0) + 1,
      last_error = excluded.last_error,
      processed = 0
  `).bind(ip, country, ts, errMsg).run();

  await safeUpdateIpLog(env, ip, 0, `queued: ${errMsg.slice(0, 200)}`);
}

async function safeUpdateIpLog(env, ip, pushedToAds, statusText) {
  try {
    await env.DB.prepare(`
      UPDATE ip_logs
      SET pushed_to_ads = ?,
          last_push_status = ?
      WHERE ip = ?
    `).bind(pushedToAds, statusText, ip).run();
  } catch (err) {
    console.log("safeUpdateIpLog failed", {
      ip,
      pushedToAds,
      statusText,
      error: normalizeError(err),
    });
  }
}

async function getAccessToken(env) {
  const body = new URLSearchParams({
    client_id: env.GOOGLE_CLIENT_ID,
    client_secret: env.GOOGLE_CLIENT_SECRET,
    refresh_token: env.GOOGLE_REFRESH_TOKEN,
    grant_type: "refresh_token",
  });

  const r = await fetch("https://oauth2.googleapis.com/token", {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body,
  });

  const txt = await r.text();

  if (!r.ok) {
    throw new Error(`Token refresh failed ${r.status}: ${txt}`);
  }

  let j;
  try {
    j = JSON.parse(txt);
  } catch {
    throw new Error(`Token refresh parse failed: ${txt}`);
  }

  if (!j?.access_token) {
    throw new Error("Token refresh returned no access_token");
  }

  return j.access_token;
}

async function addIpBlock(env, accessToken, ip) {
  const countRow = await env.DB
    .prepare("SELECT COUNT(*) AS c FROM ads_exclusions")
    .first();

  const count = Number(countRow?.c || 0);

  if (count >= 500) {
    const oldest = await env.DB.prepare(`
      SELECT ip, resource_name
      FROM ads_exclusions
      ORDER BY created_at ASC
      LIMIT 1
    `).first();

    if (oldest?.resource_name) {
      await removeCampaignCriterion(env, accessToken, oldest.resource_name);
      await env.DB.prepare("DELETE FROM ads_exclusions WHERE ip = ?").bind(oldest.ip).run();
      console.log("Removed oldest exclusion to free slot", { ip: oldest.ip });
    } else {
      throw new Error("limit reached but oldest resource_name missing");
    }
  }

  const customerId = String(env.GOOGLE_ADS_CUSTOMER_ID || "").replace(/-/g, "");
  const campaignId = String(env.GOOGLE_ADS_CAMPAIGN_ID || "").replace(/-/g, "");

  if (!customerId) {
    throw new Error("GOOGLE_ADS_CUSTOMER_ID missing");
  }

  if (!campaignId) {
    throw new Error("GOOGLE_ADS_CAMPAIGN_ID missing");
  }

  const url = `https://googleads.googleapis.com/v22/customers/${customerId}/campaignCriteria:mutate`;

  const payload = {
    customerId,
    operations: [
      {
        create: {
          campaign: `customers/${customerId}/campaigns/${campaignId}`,
          negative: true,
          ipBlock: {
            ipAddress: ip
          }
        }
      }
    ]
  };

  console.log("Sending Google Ads mutate", { customerId, campaignId, ip });

  const r = await fetch(url, {
    method: "POST",
    headers: googleHeaders(env, accessToken),
    body: JSON.stringify(payload),
  });

  const txt = await r.text();

  if (!r.ok) {
    throw new Error(`addIpBlock failed ${r.status}: ${txt}`);
  }

  let j;
  try {
    j = JSON.parse(txt);
  } catch {
    throw new Error(`addIpBlock parse failed: ${txt}`);
  }

  const resourceName = j?.results?.[0]?.resourceName;
  if (!resourceName) {
    throw new Error(`addIpBlock succeeded but resourceName missing: ${txt}`);
  }

  return resourceName;
}

async function removeCampaignCriterion(env, accessToken, resourceName) {
  const customerId = String(env.GOOGLE_ADS_CUSTOMER_ID || "").replace(/-/g, "");

  if (!customerId) {
    throw new Error("GOOGLE_ADS_CUSTOMER_ID missing");
  }

  const url = `https://googleads.googleapis.com/v22/customers/${customerId}/campaignCriteria:mutate`;

  const payload = {
    customerId,
    operations: [
      { remove: resourceName }
    ]
  };

  const r = await fetch(url, {
    method: "POST",
    headers: googleHeaders(env, accessToken),
    body: JSON.stringify(payload),
  });

  const txt = await r.text();

  if (!r.ok) {
    throw new Error(`removeCampaignCriterion failed ${r.status}: ${txt}`);
  }

  return true;
}

function googleHeaders(env, accessToken) {
  const headers = {
    "Content-Type": "application/json",
    "Authorization": `Bearer ${accessToken}`,
    "developer-token": env.GOOGLE_DEVELOPER_TOKEN,
  };

  const loginCustomerId = String(env.GOOGLE_LOGIN_CUSTOMER_ID || "").replace(/-/g, "").trim();
  if (loginCustomerId) {
    headers["login-customer-id"] = loginCustomerId;
  }

  return headers;
}

function normalizeError(err) {
  return String(err?.message || err || "unknown error");
}

function isValidIp(ip) {
  if (!ip) return false;

  const ipv4 =
    /^(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)$/;

  return ipv4.test(ip) || ip.includes(":");
}
