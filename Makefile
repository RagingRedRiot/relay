.PHONY: db seed backend backend-embedded frontend build lan-info

# ── Development (localhost only) ──────────────────────────────────────────────
#
#  Two terminals:
#    Terminal 1:  make backend   (API on :3000)
#    Terminal 2:  make frontend  (UI  on :5173, hot-reload, /ws proxied to :3000)
#
#  Visit http://localhost:5173
#  Do not use http://localhost:3000 for frontend testing in dev mode; that serves
#  the embedded frontend from the last production-style build.
#
# ── Development (LAN — test from phones / other machines) ─────────────────────
#
#  Same two-terminal setup, but Vite already listens on 0.0.0.0 so any device
#  on the same network can reach it.  The backend also binds 0.0.0.0:3000.
#
#  Run `make lan-info` to print the URL to use on other devices.
#  (Vite also prints it when it starts: look for the "Network:" line.)
#
# ── Production ────────────────────────────────────────────────────────────────
#
#  make build   →  target/release/relay
#
#  The release binary is self-contained: the frontend is embedded at compile
#  time via rust-embed. No Vite server, no static files directory needed.
#
#  Visit http://<this-machine-ip>:3000 from any device on the network.
#
# ─────────────────────────────────────────────────────────────────────────────

# Start the development database (PostgreSQL via Docker).
# Only needed if you are not running Postgres locally already.
db:
	docker compose up -d

# Populate the database with test users, rooms, and messages.
# Runs migrations first, so this is also the first-time setup step.
# Safe to re-run: existing data is left untouched.
#
#   admin  / admin123  (admin)
#   alice  / password
#   bob    / password
#   carol  / password
seed:
	cargo run --bin seed

# Start the relay backend (debug build). Run in terminal 1.
# Depends on seed so migrations and test data are always present.
backend: seed
	@echo "Backend API on http://localhost:3000. Use make frontend and open http://localhost:5173 for dev UI."
	cargo run

# Start the backend with the production-style embedded frontend on :3000.
# Use this only when you specifically want to test the embedded UI path.
backend-embedded: seed
	cd frontend && npm run build
	cargo run

# Start the Vite dev server with hot-reload. Run in terminal 2.
# Proxies /ws to the backend on :3000.
frontend:
	cd frontend && npm run dev

# Production build: compile the frontend and embed it in the release binary.
# build.rs marks frontend/dist as a Cargo input, so Cargo rebuilds the binary
# when npm produces a new bundle.
# The resulting binary at target/release/relay needs no external files.
build:
	cd frontend && npm run build
	cargo build --release

# Print the LAN URL to visit from other devices on the same network.
lan-info:
	@LAN_IP=$$(ip -4 route get 1 2>/dev/null | awk '{print $$7; exit}' || ipconfig getifaddr en0 2>/dev/null || hostname -I 2>/dev/null | awk '{print $$1}'); \
	echo ""; \
	echo "  Dev mode  →  http://$$LAN_IP:5173   (make backend + make frontend)"; \
	echo "  Prod mode →  http://$$LAN_IP:3000   (make build, then ./target/release/relay)"; \
	echo ""
