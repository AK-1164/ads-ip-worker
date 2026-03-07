
export default {
  async scheduled(event, env, ctx) {
    ctx.waitUntil(run(env));
  },
};

async function run(env) {
  const accessToken = await getAccessToken(env);

  const query = `
    SELECT customer.id, customer.descriptive_name
    FROM customer
    LIMIT 1
  `;

  const url = `https://googleads.googleapis.com/v22/customers/${env.GOOGLE_ADS_CUSTOMER_ID}/googleAds:search`;

  const r = await fetch(url, {
    method: "POST",
    headers: googleHeaders(env, accessToken),
    body: JSON.stringify({ query }),
  });

  const txt = await r.text();
  console.log("TEST STATUS:", r.status);
  console.log("TEST BODY:", txt);
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
  const headers = {
    "Content-Type": "application/json",
    "Authorization": `Bearer ${accessToken}`,
    "developer-token": env.GOOGLE_DEVELOPER_TOKEN,
  };

  if (env.GOOGLE_LOGIN_CUSTOMER_ID) {
    headers["login-customer-id"] = env.GOOGLE_LOGIN_CUSTOMER_ID;
  }

  return headers;
}
