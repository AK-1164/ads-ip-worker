
export default {
  async scheduled(event, env, ctx) {
    ctx.waitUntil(run(env));
  },
};

async function run(env) {
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

    // push:<now>:<country>:<ip>
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

      // احذف push فقط إذا نجحت الإضافة
      await kv.delete(keyName);
    } else {
      console.log("Google Ads addIpBlock failed for IP:", ip);
    }
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
    console.log(
      "addIpBlock failed",
      ip,
      "status:",
      r.status,
      JSON.stringify(j)
    );
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
