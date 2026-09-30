#!/bin/sh
# Colibri container entrypoint.
#
# Zero-config by default: generates and persists an auth token, advertises via
# STUN (works behind NAT), and runs Colibri with sane mainnet defaults. Override
# anything through the environment (see docker-compose.yml).
set -e

DATA=/data
mkdir -p "$DATA"

# ── Auth token ────────────────────────────────────────────────────────────────
# Priority: AUTH_TOKEN env  >  persisted token  >  freshly generated one.
if [ -n "$AUTH_TOKEN" ]; then
    TOKEN="$AUTH_TOKEN"
elif [ -f "$DATA/auth_token" ]; then
    TOKEN="$(cat "$DATA/auth_token")"
else
    TOKEN="$(openssl rand -hex 32)"
    printf '%s' "$TOKEN" > "$DATA/auth_token"
fi

# ── Gossip advertisement ──────────────────────────────────────────────────────
# PUBLIC_IP set -> advertise it directly; otherwise STUN discovers the NAT mapping.
if [ -n "$PUBLIC_IP" ]; then
    ADVERTISE="--ip $PUBLIC_IP"
else
    ADVERTISE="--stun ${STUN_SERVER:-stun.l.google.com:19302}"
fi

echo "================================ COLIBRI ================================"
echo "  gRPC endpoint : http://<this-host>:8888   (SubscribeEntries/Footers/Transactions)"
echo "  auth token    : $TOKEN"
echo "  subscribers send header:  authorization: Bearer <token>"
echo "  advertise     : ${PUBLIC_IP:-STUN ${STUN_SERVER:-stun.l.google.com:19302}}"
echo "========================================================================"

# Entrypoints default to the five mainnet-beta gossip nodes inside Colibri, so
# they are not passed here. exec so signals reach Colibri (clean shutdown).
exec ./colibri \
    $ADVERTISE \
    --grpc-port 8888 \
    --rpc "${SOLANA_RPC:-https://api.mainnet-beta.solana.com}" \
    --auth-token "$TOKEN" \
    --keypair "$DATA/colibri-identity.json" \
    --depth "${DEPTH:-500}"
