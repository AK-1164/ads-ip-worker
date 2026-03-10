export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    if (url.pathname === "/do-check") {
      return handleDoCheck(request, env);
    }

    return handleFetch(request, env, ctx);
  },

  async scheduled(event, env, ctx) {
    ctx.waitUntil(runRetryQueue(env));
  },
};

async function handleFetch(request, env, ctx) {
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
    const accessToken = await getAccessToken(env);
    const ok = await processSingleIp(env, accessToken, ip);

    if (ok) {
      console.log("Direct add success:", ip);
      return new Response("OK", { status: 200 });
    }

    // فشل الإضافة → خزّن push احتياطيًا
    const key = `push:${ts}:${country}:${ip}`;
    await env.VISITS.put(key, "1", { expirationTtl: 7 * 24 * 3600 });

    console.log("Direct add failed, saved to retry queue:", ip);
    return new Response("Queued", { status: 202 });
  } catch (err) {
    // أي خطأ عام → خزّن push احتياطيًا
    const key = `push:${ts}:${country}:${ip}`;
    await env.VISITS.put(key, "1", { expirationTtl: 7 * 24 * 3600 });

    console.log("Direct worker exception, saved to retry queue:", ip, String(err));
    return new Response("Queued", { status: 202 });
  }
}

async function runRetryQueue(env) {
  const kv = env.VISITS;

  const batch = await kv.list({ prefix: "push:", limit: 50 });
  if (!batch.keys || batch.keys.length === 0) return;

  const accessToken = await getAccessToken(env);

  let order = [];
  const orderRaw = await kv.get("ads:order");
  if (orderRaw) {
    try {
      order = JSON.parse(orderRaw);
    } catch {}
  }
  if (!Array.isArray(order)) order = [];

  for (const k of batch.keys) {
    const keyName = k.name;

    const parts = keyName.split(":");
    const ip = parts.slice(3).join(":"); // يدعم IPv6
    if (!ip) {
      await kv.delete(keyName);
      continue;
    }

    const doneKey = `ads:done:${ip}`;
    const alreadyDone = await kv.get(doneKey);
    if (alreadyDone) {
      await kv.delete(keyName);
      continue;
    }

    const count = await countIpBlocks(env, accessToken);
    if (count >= 500) {
      await removeOldest(env, accessToken, kv, order);
    }

    const resourceName = await addIpBlock(env, accessToken, ip);

    if (resourceName) {
      order.push(ip);
      await kv.put("ads:order", JSON.stringify(order));
      await kv.put(`ads:ip:${ip}`, resourceName);
      await kv.put(doneKey, "1", { expirationTtl: 7 * 24 * 3600 });
      await kv.delete(keyName);
      console.log("Retry add success:", ip);
    } else {
      console.log("Retry add failed:", ip);
    }
  }
}

async function processSingleIp(env, accessToken, ip) {
  const kv = env.VISITS;

  const doneKey = `ads:done:${ip}`;
  const alreadyDone = await kv.get(doneKey);
  if (alreadyDone) {
    console.log("Already done, skip:", ip);
    return true;
  }

  let order = [];
  const orderRaw = await kv.get("ads:order");
  if (orderRaw) {
    try {
      order = JSON.parse(orderRaw);
    } catch {}
  }
  if (!Array.isArray(order)) order = [];

  const count = await countIpBlocks(env, accessToken);
  if (count >= 500) {
    await removeOldest(env, accessToken, kv, order);
  }

  const resourceName = await addIpBlock(env, accessToken, ip);
  if (!resourceName) return false;

  order.push(ip);
  await kv.put("ads:order", JSON.stringify(order));
  await kv.put(`ads:ip:${ip}`, resourceName);
  await kv.put(doneKey, "1", { expirationTtl: 7 * 24 * 3600 });

  return true;
}

async function handleDoCheck(request, env) {
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

  const ip = body?.ip || "unknown";
  const country = body?.country || "XX";

  const id = env.IP_GUARD.idFromName(ip);
  const stub = env.IP_GUARD.get(id);

  return stub.fetch("https://dummy/do", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ ip, country }),
  });
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

  return j?.results?.[0]?.resourceName || null;
}

async function removeOldest(env, accessToken, kv, order) {
  while (order.length > 0) {
    const oldestIp = order.shift();
    const resName = await kv.get(`ads:ip:${oldestIp}`);
    await kv.delete(`ads:ip:${oldestIp}`);

    if (resName) {
      await removeCampaignCriterion(env, accessToken, resName);
      await kv.put("ads:order", JSON.stringify(order));
      return;
    }
  }

  await kv.put("ads:order", JSON.stringify(order));
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

async function countIpBlocks(env, accessToken) {
  const query = `
    SELECT campaign_criterion.resource_name
    FROM campaign_criterion
    WHERE campaign_criterion.campaign = 'customers/${env.GOOGLE_ADS_CUSTOMER_ID}/campaigns/${env.GOOGLE_ADS_CAMPAIGN_ID}'
      AND campaign_criterion.negative = TRUE
      AND campaign_criterion.type = IP_BLOCK
    LIMIT 1000
  `;

  const url = `https://googleads.googleapis.com/v22/customers/${env.GOOGLE_ADS_CUSTOMER_ID}/googleAds:search`;

  const r = await fetch(url, {
    method: "POST",
    headers: googleHeaders(env, accessToken),
    body: JSON.stringify({ query }),
  });

  const j = await r.json();

  if (!r.ok) {
    console.log("countIpBlocks failed", r.status, JSON.stringify(j));
    return 0;
  }

  return j?.results?.length || 0;
}

function googleHeaders(env, accessToken) {
  return {
    "Content-Type": "application/json",
    "Authorization": `Bearer ${accessToken}`,
    "developer-token": env.GOOGLE_DEVELOPER_TOKEN,
    "login-customer-id": "1486808188"
  };
}

export class IpGuard {
  constructor(ctx, env) {
    this.ctx = ctx;
    this.env = env;
    this.storage = ctx.storage;
  }

  async fetch(request) {
    if (request.method !== "POST") {
      return new Response("Method Not Allowed", { status: 405 });
    }

    let body;
    try {
      body = await request.json();
    } catch {
      return new Response("Invalid JSON", { status: 400 });
    }

    const ip = body?.ip || "unknown";
    const country = body?.country || "XX";

    const now = Math.floor(Date.now() / 1000);
    const windowSeconds = 24 * 3600;
    const banSeconds = 7 * 24 * 3600;

    const state = (await this.storage.get("state")) || {
      ip,
      country,
      count: 0,
      firstSeen: now,
      lastSeen: now,
      bannedUntil: 0,
      pushNeeded: false
    };

    state.lastSeen = now;
    state.ip = ip;
    state.country = country;

    // اليمن: حظر مباشر
    if (country === "YE") {
      state.bannedUntil = now + banSeconds;
      state.pushNeeded = true;

      await this.storage.put("state", state);

      return Response.json({
        ok: true,
        action: "ban",
        reason: "YE",
        pushNeeded: true
      });
    }

    // غير السعودية: دخول عادي بدون عداد
    if (country !== "SA") {
      await this.storage.put("state", state);

      return Response.json({
        ok: true,
        action: "allow",
        pushNeeded: false
      });
    }

    // إذا كان محظورًا مسبقًا
    if (state.bannedUntil && state.bannedUntil > now) {
      state.count += 1;
      await this.storage.put("state", state);

      return Response.json({
        ok: true,
        action: "ban",
        reason: "already_banned",
        pushNeeded: false
      });
    }

    // إعادة ضبط العداد بعد 24 ساعة
    if (!state.firstSeen || (now - state.firstSeen) > windowSeconds) {
      state.count = 0;
      state.firstSeen = now;
    }

    state.count += 1;

    // بعد المرتين: حظر
    if (state.count > 2) {
      state.bannedUntil = now + banSeconds;
      state.pushNeeded = true;

      await this.storage.put("state", state);

      return Response.json({
        ok: true,
        action: "ban",
        reason: "too_many_clicks",
        pushNeeded: true
      });
    }

    await this.storage.put("state", state);

    return Response.json({
      ok: true,
      action: "allow",
      pushNeeded: false
    });
  }
}