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
};

async function createCheckoutSession({
  customerId,
  userId,
  seats = 1,
  planCode = "launch",
  req,
}) {
  // upsert Stripe customer (one per tenant is typical; we’re storing it on stripe_user)
  // you might already have a stripeCustomerId for this tenant; re-use if so
  const stripeCustomer = await stripe.customers.create({
    metadata: { customerId, userId, planCode, seats: String(seats) },
  });

  const params = {
    mode: "subscription",
    success_url: successUrl,
    cancel_url: cancelUrl,
    allow_promotion_codes: true,
    automatic_tax: { enabled: true },
    line_items: [{ price: process.env.PRICE_STANDARD_ID, quantity: 1 }], // per-tenant subscription; enforce "20 users included" in app
    client_reference_id: customerId,
    customer: stripeCustomer.id,
    // optional: subscription_data: { trial_period_days: 0 }
  };

  // If you want to pre-attach a promo code (instead of user entering it), you can add discount here.
  if (process.env.LAUNCH_PROMO_CODE && planCode === "launch") {
    // leave it to user to enter promo; cleaner: keep just allow_promotion_codes: true
  }

  // Stripe idempotency (one session per request body)
  const session = await stripe.checkout.sessions.create(params, {
    idempotencyKey: `chk_${customerId}_${userId}_${Date.now()}`, // improve with a stable hash of inputs
  });

  // create a stripe_user row now if you want, with customerId/userId/createdBy; the webhook will fill Stripe IDs
  await db.StripeUser.create({ customerId, userId, createdBy: userId }).catch(
    () => null
  );

  return session;
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

async function handleWebhook({ rawBody, sig }) {
  const webhookSecret = process.env.STRIPE_WEBHOOK_SECRET;
  let event;
  try {
    event = stripe.webhooks.constructEvent(rawBody, sig, webhookSecret);
  } catch (err) {
    err.statusCode = 400;
    throw err;
  }

  switch (event.type) {
    case "checkout.session.completed": {
      const session = event.data.object;
      const stripeCustomerId = session.customer;
      const subscriptionId = session.subscription;
      const priceId =
        session.mode === "subscription"
          ? (await stripe.subscriptions.retrieve(subscriptionId)).items.data[0]
              .price.id
          : null;

      // customerId was set in metadata when we created the customer
      const stripeCustomer = await stripe.customers.retrieve(stripeCustomerId);
      const customerId = stripeCustomer.metadata?.customerId;
      const userId = stripeCustomer.metadata?.userId;

      // update tbl_stripe_user + activate tenant
      const t = await beginTransactionWithCustomerContext(customerId);
      try {
        const su = await db.StripeUser.findOne({
          where: { customerId, userId },
          transaction: t,
        });
        if (su) {
          await su.update(
            {
              stripeCustomerId,
              stripeSubscriptionId: subscriptionId,
              stripePriceId: priceId,
              status: "active",
              isActive: true,
              updatedBy: userId,
            },
            { transaction: t }
          );
        }
        // flip your tenant/customer row to active (whatever your field is)
        if (db.Customer) {
          await db.Customer.update(
            { status: "active" },
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
      const priceId = sub.items?.data?.[0]?.price?.id;

      const stripeCustomer = await stripe.customers.retrieve(stripeCustomerId);
      const customerId = stripeCustomer.metadata?.customerId;

      const t = await beginTransactionWithCustomerContext(customerId);
      try {
        await db.StripeUser.update(
          {
            status,
            stripePriceId: priceId,
            isActive: status === "active" || status === "trialing",
          },
          { where: { stripeSubscriptionId: sub.id }, transaction: t }
        );
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
}
