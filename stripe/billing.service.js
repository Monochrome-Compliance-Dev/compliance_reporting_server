const stripe = require("stripe")(process.env.STRIPE_SECRET_KEY, {
  apiVersion: "2024-06-20",
});
const db = require("../db/database");
const {
  beginTransactionWithCustomerContext,
} = require("../helpers/setCustomerIdRLS");

const successUrl =
  process.env.APP_BASE_URL + "/welcome?session_id={CHECKOUT_SESSION_ID}";
const cancelUrl = process.env.APP_BASE_URL + "/register?canceled=1";

module.exports = {
  createCheckoutSession,
  verifySession,
  createPortalSession,
  handleWebhook,
  getEntitlements,
};

async function createCheckoutSession({
  customerId,
  userId,
  seats = 1,
  planCode = "launch",
  req,
}) {
  // Prefill address/name/email/phone from our app Customer record if available
  let appCustomer = null;
  try {
    appCustomer = await db.Customer.findOne({ where: { id: customerId } });
  } catch (_) {
    // Non-fatal: continue without prefill
  }
  const addressPayload = appCustomer
    ? {
        name:
          appCustomer.customerName ||
          appCustomer.businessName ||
          appCustomer.company ||
          undefined,
        email: appCustomer.email || undefined,
        phone: appCustomer.phone || undefined,
        address: {
          line1: appCustomer.addressline1 || undefined,
          line2: appCustomer.addressline2 || undefined,
          city: appCustomer.city || undefined,
          state: appCustomer.state || undefined,
          postal_code: appCustomer.postcode || undefined,
          country:
            appCustomer.country === "Australia"
              ? "AU"
              : appCustomer.country || undefined,
        },
      }
    : {};

  // Reuse existing Stripe customer if linked; otherwise create with prefilled address
  let stripeCustomerId;
  const existingLink = await db.StripeUser.findOne({
    where: { customerId, userId },
    order: [["createdAt", "DESC"]],
  }).catch(() => null);

  if (existingLink?.stripeCustomerId) {
    stripeCustomerId = existingLink.stripeCustomerId;
  } else {
    const createdCustomer = await stripe.customers.create({
      ...addressPayload,
      metadata: { customerId, userId, planCode, seats: String(seats) },
    });
    stripeCustomerId = createdCustomer.id;
  }

  const params = {
    mode: "subscription",
    success_url: successUrl,
    cancel_url: cancelUrl,
    allow_promotion_codes: true,
    automatic_tax: { enabled: true },
    customer: stripeCustomerId,
    client_reference_id: customerId,
    line_items: [{ price: process.env.PRICE_STANDARD_ID, quantity: seats }],
    // Let Checkout collect and save the address to the Customer (satisfies automatic tax)
    customer_update: { address: "auto", shipping: "auto" },
    // AU GST: restrict address collection to AU. Add more if you sell in other regions.
    shipping_address_collection: { allowed_countries: ["AU"] },
    // optional: subscription_data: { trial_period_days: 0 },
  };

  // If you want to pre-attach a promo code (instead of user entering it), you can add discount here.
  if (process.env.LAUNCH_PROMO_CODE && planCode === "launch") {
    // leave it to user to enter promo; cleaner: keep just allow_promotion_codes: true
  }

  // Stripe idempotency (one session per request body)
  const session = await stripe.checkout.sessions.create(params, {
    idempotencyKey: `chk_${customerId}_${userId}_${Date.now()}`, // improve with a stable hash of inputs
  });

  // Ensure stripe_user linkage exists (no-op if already there)
  await db.StripeUser.findOrCreate({
    where: { customerId, userId },
    defaults: { createdBy: userId, stripeCustomerId },
  }).catch(() => null);

  return { id: session.id, url: session.url };
}

async function verifySession({ sessionId }) {
  const session = await stripe.checkout.sessions.retrieve(sessionId, {
    expand: ["subscription", "customer"],
  });
  return {
    status: session.status,
    customer_email: session.customer_details?.email,
    subscription_status: session.subscription?.status,
  };
}

async function createPortalSession({ customerId, returnUrl }) {
  // lookup the tenant’s stripeCustomerId from tbl_stripe_user
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const su = await db.StripeUser.findOne({
      where: { customerId },
      order: [["createdAt", "DESC"]],
      transaction: t,
    });
    if (!su || !su.stripeCustomerId)
      throw new Error("No Stripe customer on record.");
    const session = await stripe.billingPortal.sessions.create({
      customer: su.stripeCustomerId,
      return_url: returnUrl,
    });
    await t.commit();
    return session.url;
  } catch (e) {
    await t.rollback();
    throw e;
  }
}

async function getEntitlements({ customerId }) {
  // seats come from tbl_customer; used is count of active users
  const customer = await db.Customer.findOne({
    where: { id: customerId },
    attributes: ["seats"],
  });
  const seats = customer?.seats ?? 1;

  const used = await db.User.count({
    where: { customerId, active: true },
  });

  const remaining = Math.max(0, seats - used);
  return { seats, used, remaining };
}

async function handleWebhook({ rawBody, sig }) {
  const webhookSecret = process.env.STRIPE_WEBHOOK_SECRET;
  let event;
  try {
    event = stripe.webhooks.constructEvent(rawBody, sig, webhookSecret);
  } catch (err) {
    err.statusCode = 400;
    throw err;
  }

  // Idempotency guard: prevent double-processing on Stripe retries
  try {
    if (db.WebhookEvent && typeof db.WebhookEvent.findOrCreate === "function") {
      const [rec, created] = await db.WebhookEvent.findOrCreate({
        where: { eventId: event.id },
        defaults: {
          eventId: event.id,
          type: event.type,
          processedAt: new Date(),
        },
      });
      if (!created) {
        // Already processed this event; report duplicate to controller
        return {
          processed: false,
          reason: "duplicate",
          eventId: event.id,
          type: event.type,
        };
      }
    }
  } catch (_) {
    // If the table doesn't exist or fails, proceed without idempotency rather than breaking billing.
  }

  switch (event.type) {
    case "checkout.session.completed": {
      const session = event.data.object;
      const stripeCustomerId = session.customer;
      const subscriptionId = session.subscription;

      // Pull full subscription for price, quantity (seats), status and item id
      const sub = await stripe.subscriptions.retrieve(subscriptionId);
      const item = sub.items.data[0];
      const priceId = item?.price?.id || null;
      const subscriptionItemId = item?.id || null;
      const quantity = item?.quantity ?? 1;
      const status = sub.status; // trialing | active | past_due | canceled | incomplete | ...

      // customerId/userId were placed in Stripe customer metadata during session creation
      const stripeCustomer = await stripe.customers.retrieve(stripeCustomerId);
      const customerId = stripeCustomer.metadata?.customerId;
      const userId = stripeCustomer.metadata?.userId;

      // update tbl_stripe_user + tenant state + seats (within RLS-aware txn)
      const t = await beginTransactionWithCustomerContext(customerId);
      try {
        await db.StripeUser.update(
          {
            stripeCustomerId,
            stripeSubscriptionId: subscriptionId,
            stripeSubscriptionItemId: subscriptionItemId,
            stripePriceId: priceId,
            status,
            isActive: status === "active" || status === "trialing",
            updatedBy: userId,
          },
          { where: { customerId, userId }, transaction: t }
        );

        if (db.Customer) {
          await db.Customer.update(
            {
              status: status === "canceled" ? "canceled" : "active",
              seats: quantity,
            },
            { where: { id: customerId }, transaction: t }
          );
        }
        await t.commit();
      } catch (e) {
        await t.rollback();
        throw e;
      }
      break;
    }

    case "customer.subscription.updated":
    case "customer.subscription.deleted": {
      const sub = event.data.object;
      const stripeCustomerId = sub.customer;
      const status = sub.status; // trialing | active | past_due | canceled | incomplete | ...
      const item = sub.items?.data?.[0];
      const priceId = item?.price?.id || null;
      const subscriptionItemId = item?.id || null;
      const quantity = item?.quantity ?? null;

      const stripeCustomer = await stripe.customers.retrieve(stripeCustomerId);
      const customerId = stripeCustomer.metadata?.customerId;

      const t = await beginTransactionWithCustomerContext(customerId);
      try {
        await db.StripeUser.update(
          {
            status,
            stripePriceId: priceId,
            stripeSubscriptionItemId: subscriptionItemId,
            isActive: status === "active" || status === "trialing",
          },
          { where: { stripeSubscriptionId: sub.id }, transaction: t }
        );

        if (db.Customer && quantity != null) {
          await db.Customer.update(
            {
              seats: quantity,
              status: status === "canceled" ? "canceled" : undefined,
            },
            { where: { id: customerId }, transaction: t }
          );
        }

        await t.commit();
      } catch (e) {
        await t.rollback();
        throw e;
      }
      break;
    }

    case "invoice.payment_failed": {
      // Optional: mark past_due and notify
      break;
    }
    default:
      // ignore the rest
      break;
  }
  return { processed: true, eventId: event.id, type: event.type };
}
