# 🎬 QueueDeck

> A fast, self-hosted media dashboard for tracking what to watch next — powered by Sonarr, Radarr, Jellyfin, and more.

QueueDeck brings all your media into one clean, modern interface so you can instantly see:

* 📺 What’s next to watch
* 🎞️ Recently added movies
* 📡 Upcoming episodes
* ❗ Missing content
* 🔥 Discover new shows and movies

Built for homelab users who want something **faster, cleaner, and more customizable** than traditional dashboards.

---

# ✨ Features

* 🎯 **Unified Dashboard**

  * Continue Watching
  * Next Up (Sonarr)
  * Recently Added (Radarr)
  * Upcoming Episodes
  * Missing Content

* 🔍 **Discover Page**

  * Trending movies & TV
  * Anime integration
  * External metadata enrichment (TMDB)

* 📡 **RSS Feeds (Unique Feature)**

  * Export your queues as RSS feeds
  * Use with external tools, dashboards, or notifications
  * Turn QueueDeck into a **data source**, not just a UI

* 🧠 **Smart UI**

  * Hover animations
  * “New episode” indicators
  * Clean card-based layout

* 🔐 **Authentication System**

  * Secure login system
  * Admin panel
  * Session protection

* ⚙️ **Settings & Customization**

  * Adjustable limits per section
  * Jellyfin integration
  * External API configuration

* 🐳 **Docker-First**

  * Simple deployment
  * Lightweight image
  * Works great behind reverse proxies

---

# 🤖 AI Usage

QueueDeck was developed with the assistance of AI tools to accelerate development, improve code quality, and iterate quickly on features.

All logic, integrations, and architecture have been reviewed and tested in real-world homelab environments.

---

# 🔒 Security

QueueDeck is designed to be safely exposed behind a reverse proxy.

* ✅ Gunicorn production server
* ✅ Secure session cookies
* ✅ Login rate limiting (anti brute-force)
* ✅ Admin route protection
* ✅ SSRF protection (Letterboxd RSS)
* ✅ No secrets stored in repo

> ⚠️ Recommended: run behind a reverse proxy (Caddy / Nginx / Traefik)

---

# 🚀 Quick Start (Docker)

## 1. Create a directory

```bash
mkdir queuedeck && cd queuedeck
```

## 2. Create `docker-compose.yml`

```yaml
services:
  queuedeck:
    image: reverberation99/queuedeck:latest
    container_name: queuedeck

    ports:
      - "7071:7071"

    volumes:
      - ./data:/data

    environment:
      HOST: "0.0.0.0"
      PORT: "7071"
      APP_NAME: "QueueDeck"
      SECRET_KEY: "change-me"
      DB_PATH: "/data/queuedb.sqlite"

    restart: unless-stopped
```

## 3. Start it

```bash
docker compose up -d
```

## 4. Open in browser

```
http://localhost:7071
```

First run will prompt you to create an admin account.

---

# ⚙️ Environment Variables

| Variable     | Description                         |
| ------------ | ----------------------------------- |
| `SECRET_KEY` | Required. Used for session security |
| `DB_PATH`    | SQLite database location            |
| `PORT`       | Internal app port (default: 7071)   |
| `HOST`       | Bind address (default: 0.0.0.0)     |

---

# 🔌 Integrations

QueueDeck supports:

* 📺 Sonarr
* 🎬 Radarr
* 🍿 Jellyfin
* 🎥 TMDB
* 📡 Letterboxd RSS

---

# 📡 RSS Output

QueueDeck can expose your media queues as RSS feeds, allowing you to:

* Plug into external dashboards (Homepage, Homarr, etc.)
* Trigger automations
* Monitor activity outside the UI

This makes QueueDeck not just a dashboard — but a **data hub for your media ecosystem**.

---

# 📁 Data

All persistent data is stored in:

```
/data/queuedb.sqlite
```

👉 Back this file up regularly.

---

# 🛠 Development

Clone the repo and run:

```bash
docker compose up --build
```

---

# 📸 Screenshots

> (add your screenshots here — dashboard, discover page, etc.)

---

# 🗺 Roadmap

* [ ] Shared cache (Redis)
* [ ] Multi-user roles
* [ ] UI customization themes
* [ ] Notifications / alerts
* [ ] Performance improvements for Discover

---

# 🤝 Contributing

Contributions are welcome — feel free to open issues or PRs.

---

# 📄 License

MIT (or whatever you choose)

---

# 🧡 Acknowledgements

Built for the self-hosted / homelab community.

---

# ⭐ If you like QueueDeck

Give it a star — it helps a lot!
