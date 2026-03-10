export default {
  async fetch(request, env, ctx) {
    return handleFetch(request, env);
  },

  async scheduled(event, env, ctx) {
    ctx.waitUntil(runRetryQueue(env));
  },
};

async function handleFetch(request, env) {
  if (request.method !== "POST") {
    return new Response("Method Not Allowed", { status: 405 });
  }

  const secret = request.headers.get("x-worker-secret");
  if (!secret || secret !== env.WORKER_SHARED_SECRET) {
    return new Response("Unauthorized", { status: 401 });
  }

  let body;
  try {
    body = await request.json();
  } catch {
    return new Response("Invalid JSON", { status: 400 });
  }

  const ip = body?.ip;
  const country = body?.country || "XX";
  const ts = body?.ts || Math.floor(Date.now() / 1000);

  if (!ip) {
    return new Response("Missing IP", { status: 400 });
  }

  try {
    // إذا كان مستبعدًا مسبقًا في D1، لا نكرر الإضافة
    const existing = await env.DB
      .prepare("SELECT ip FROM ads_exclusions WHERE ip = ?")
      .bind(ip)
      .first();

    if (existing) {
      await safeUpdateIpLog(env, ip, 1, "already_excluded");
      return new Response("Already excluded", { status: 200 });
    }

    const accessToken = await getAccessToken(env);
    const resourceName = await addIpBlock(env, accessToken, ip);

    if (!resourceName) {
      await queueFailure(env, ip, country, ts, "addIpBlock returned null");
      return new Response("Queued", { status: 202 });
    }

    await env.DB.prepare(`
      INSERT INTO ads_exclusions (ip, resource_name, created_at)
      VALUES (?, ?, ?)
      ON CONFLICT(ip) DO UPDATE SET
        resource_name = excluded.resource_name,
        created_at = excluded.created_at
    `).bind(ip, resourceName, ts).run();

    await safeUpdateIpLog(env, ip, 1, "success");
    await env.DB.prepare("DELETE FROM exclusion_queue WHERE ip = ?").bind(ip).run();

    console.log("Direct add success:", ip);
    return new Response("OK", { status: 200 });
  } catch (err) {
    const msg = String(err?.message || err || "unknown error");
    await queueFailure(env, ip, country, ts, msg);
    console.log("Direct worker exception, queued:", ip, msg);
    return new Response("Queued", { status: 202 });
  }
}

async function runRetryQueue(env) {
  const rows = await env.DB.prepare(`
    SELECT ip, country, created_at
    FROM exclusion_queue
    WHERE processed = 0
    ORDER BY created_at ASC
    LIMIT 20
  `).all();

  if (!rows || !rows.results || rows.results.length === 0) return;

  const accessToken = await getAccessToken(env);

  for (const row of rows.results) {
    const ip = row.ip;
    const country = row.country || "XX";
    const ts = row.created_at || Math.floor(Date.now() / 1000);

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
        continue;
      }

      const resourceName = await addIpBlock(env, accessToken, ip);

      if (!resourceName) {
        await queueFailure(env, ip, country, ts, "retry addIpBlock returned null");
        console.log("Retry add failed:", ip);
        continue;
      }

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
      console.log("Retry add success:", ip);
    } catch (err) {
      const msg = String(err?.message || err || "retry error");
      await queueFailure(env, ip, country, ts, msg);
      console.log("Retry exception:", ip, msg);
    }
  }
}

async function queueFailure(env, ip, country, ts, errorText) {
  await env.DB.prepare(`
    INSERT INTO exclusion_queue (ip, country, created_at, attempts, last_error, processed)
    VALUES (?, ?, ?, 1, ?, 0)
    ON CONFLICT(ip) DO UPDATE SET
      country = excluded.country,
      created_at = excluded.created_at,
      attempts = exclusion_queue.attempts + 1,
      last_error = excluded.last_error,
      processed = 0
  `).bind(ip, country, ts, String(errorText).slice(0, 1000)).run();

  await safeUpdateIpLog(env, ip, 0, `queued: ${String(errorText).slice(0, 200)}`);
}

async function safeUpdateIpLog(env, ip, pushedToAds, statusText) {
  await env.DB.prepare(`
    UPDATE ip_logs
    SET pushed_to_ads = ?,
        last_push_status = ?
    WHERE ip = ?
  `).bind(pushedToAds, statusText, ip).run();
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

  if (!r.ok) {
    const txt = await r.text();
    throw new Error(`Token refresh failed: ${txt}`);
  }

  const j = await r.json();
  return j.access_token;
}

async function addIpBlock(env, accessToken, ip) {
  // لا نكرر الإضافة إذا وصل العدد 500 نحذف الأقدم أولًا
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

    if (oldest && oldest.resource_name) {
      await removeCampaignCriterion(env, accessToken, oldest.resource_name);
      await env.DB.prepare("DELETE FROM ads_exclusions WHERE ip = ?").bind(oldest.ip).run();
    }
  }

  const url = `https://googleads.googleapis.com/v22/customers/${env.GOOGLE_ADS_CUSTOMER_ID}/campaignCriteria:mutate`;

  const payload = {
    customerId: String(env.GOOGLE_ADS_CUSTOMER_ID),
    operations: [
      {
        create: {
          campaign: `customers/${env.GOOGLE_ADS_CUSTOMER_ID}/campaigns/${env.GOOGLE_ADS_CAMPAIGN_ID}`,
          negative: true,
          ipBlock: {
            ipAddress: ip
          }
        }
      }
    ]
  };

  const r = await fetch(url, {
    method: "POST",
    headers: googleHeaders(env, accessToken),
    body: JSON.stringify(payload),
  });

  const j = await r.json();

  if (!r.ok) {
    console.log("addIpBlock failed", ip, "status:", r.status, JSON.stringify(j));
    return null;
  }

  return j && j.results && j.results[0] && j.results[0].resourceName
    ? j.results[0].resourceName
    : null;
}

async function removeCampaignCriterion(env, accessToken, resourceName) {
  const url = `https://googleads.googleapis.com/v22/customers/${env.GOOGLE_ADS_CUSTOMER_ID}/campaignCriteria:mutate`;

  const payload = {
    customerId: String(env.GOOGLE_ADS_CUSTOMER_ID),
    operations: [
      { remove: resourceName }
    ]
  };

  const r = await fetch(url, {
    method: "POST",
    headers: googleHeaders(env, accessToken),
    body: JSON.stringify(payload),
  });

  if (!r.ok) {
    const txt = await r.text();
    console.log("removeCampaignCriterion failed", r.status, txt);
  }
}

function googleHeaders(env, accessToken) {
  return {
    "Content-Type": "application/json",
    "Authorization": `Bearer ${accessToken}`,
    "developer-token": env.GOOGLE_DEVELOPER_TOKEN,
    "login-customer-id": env.GOOGLE_LOGIN_CUSTOMER_ID
  };
}
