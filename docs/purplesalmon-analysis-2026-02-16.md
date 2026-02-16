# PurpleSalmon Reverse Engineering Notes (2026-02-16)

## Scope

This note captures what was directly observed from `https://purplesalmon.xyz/` and its shipped frontend bundle on February 16, 2026.

## Observed facts

- Hosting and app stack:
  - Static frontend served by Vercel.
  - React single-page app bundle (`/assets/index-DADc-K_u.js`).
  - Tailwind-style utility classes in markup.
- Backend/data service:
  - Supabase project URL embedded in frontend.
  - Public anon key embedded in frontend (normal for Supabase client apps).
  - Supabase auth session handling in browser.
- Public database usage found in bundle:
  - Tables queried from client:
    - `featured_position`
    - `positions`
    - `contact_submissions`
  - Edge Functions invoked from client:
    - `check-subscription`
    - `create-checkout`
- Routing/features found in bundle:
  - Routes: `/`, `/auth`, `/vault`, `/admin`, `/plans`.
  - Admin route writes directly to `positions` table.
  - Vault route reads active positions and shows trade narratives.
- Content pattern:
  - Claims transparency and "live positions".
  - Shows narrative blocks such as "The Macro Reality" and "The Trade".
  - Includes external data-source links (example present: Atlanta Fed GDPNow).

## Important clarification for our project

- As observed on 2026-02-16, the live PurpleSalmon content appears focused on macro/GDP-position narratives, not an obvious weather-model interface.
- I did **not** find direct Kalshi API endpoint calls in the frontend bundle (no visible `api.kalshi.com` call path in shipped client code).
- This strongly suggests the public product layer is primarily:
  - a transparency/publishing dashboard, plus
  - subscription/auth workflows,
  - with position data managed via Supabase (and likely manual or semi-manual updates on the visible layer).

## Inference (not directly provable from frontend alone)

- The "AI engine" could exist off-frontend, but the shipped web app itself does not expose model pipeline details.
- The product can be replicated structurally by building:
  - data/model backend + trading pipeline, and
  - a transparent frontend that publishes signals, positions, confidence, and rationale.
