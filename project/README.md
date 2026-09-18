# Lakehouse Analytics — Flask / MinIO / PostgreSQL / Spark

Platform data lakehouse internal untuk data operasional perusahaan (network
operation, customer service, finance, inventory). Staf mengunggah CSV
mentah; sistem otomatis memvalidasi, membersihkan, dan menggabungkannya ke
dalam satu dataset "Master" per format, lalu menghitung KPI/chart secara
langsung saat dibutuhkan.

Browser (HTML/CSS/JS) **hanya** memanggil Flask REST API ini — tidak pernah
berbicara langsung ke MinIO, PostgreSQL, atau Spark.

```
Browser --fetch()--> Nginx (load balancer, :80) --> app1 / app2 / app3 (Flask + Gunicorn)
                                                        ├── routes/auth.py     (login/session)
                                                        ├── routes/datasets.py (katalog, preview, visualize, merge)
                                                        ├── routes/upload.py   (validasi + upload ke Bronze/Master)
                                                        ├── routes/etl.py      (pembersihan via Spark -> Silver)
                                                        └── routes/admin.py    (edit/hapus baris, hapus dataset, audit log)
                                                        |
                                                  services/
                                                        ├── minio_service.py    (satu-satunya modul pengimpor boto3)
                                                        ├── postgres_service.py (satu-satunya modul pengimpor psycopg2)
                                                        ├── spark_service.py    (logika pembersihan/merge/agregasi)
                                                        └── catalog_service.py  (gabungan isi MinIO + metadata Postgres)
```

`app1`/`app2`/`app3` adalah tiga container identik; Nginx satu-satunya pintu
masuk dan membagi beban di antara ketiganya. Tidak ada container app yang
membuka port langsung ke luar — semua lewat Nginx di port 80.

---

## 1. Cara Menjalankan di Komputer Lokal (Docker Compose)

Cara paling mudah — menjalankan PostgreSQL, MinIO, tiga replica app, dan
Nginx sekaligus. Schema database diterapkan otomatis, dan akun Administrator
pertama langsung dibuatkan.

```bash
docker compose up --build -d
```

Lalu buka **http://localhost** (port 80 lewat Nginx — **bukan** `:5000`,
container app tidak bisa diakses langsung dari luar). Login memakai
`SEED_ADMIN_USERNAME` / `SEED_ADMIN_PASSWORD` dari file `.env`.

> Ganti dulu `SECRET_KEY` dan `SEED_ADMIN_PASSWORD` di `.env` sebelum dipakai
> untuk hal yang serius — nilai default di repo ini hanya untuk coba-coba di
> lokal.

Catatan singkat:
- Schema Postgres hanya diterapkan otomatis pada **pertama kali** volume
  `pg_data` masih kosong. Untuk reset total (hapus semua data):
  `docker compose down -v`.
- Setiap kali mengedit file `.py`, `templates/`, atau `static/`, perubahan
  **tidak** langsung terlihat — wajib rebuild dulu (lihat bagian 6).

---

## 2. Membuat Akun Pengguna

Tidak ada halaman "daftar akun" — pembuatan akun lewat command line, oleh
orang yang punya akses ke container.

**Akun Administrator pertama** — otomatis dibuat oleh service `seed` saat
pertama kali `docker compose up` dijalankan (pakai `SEED_ADMIN_*` di `.env`).

**Akun tambahan** (Employee atau Administrator lain) — pakai `db/create_user.py`:

```bash
docker compose exec app1 python db/create_user.py <username> <password> <role> [nama_lengkap]

# contoh:
docker compose exec app1 python db/create_user.py jsmith "password-kuat" Employee "Jane Smith"
```

`<role>` harus persis `Administrator` atau `Employee`. Cukup jalankan
sekali per akun, ke salah satu dari `app1`/`app2`/`app3` — ketiganya
berbagi database yang sama.

---

## 3. Peran & Hak Akses

| | Employee | Administrator |
|---|---|---|
| Login, upload data | ✅ | ✅ |
| Lihat/eksplor dataset | ✅ | ✅ |
| Cleaning manual & Visualize | ✅ | ✅ |
| Merge dataset manual | ❌ | ✅ |
| Edit/hapus baris, hapus dataset | ❌ | ✅ |
| Lihat audit log | ❌ | ✅ |

Ditegakkan di server lewat `admin_required` (`routes/_common.py`) — bukan
hanya disembunyikan di tampilan. UI ikut menyembunyikan tombol admin-only
untuk Employee, tapi itu sekadar kenyamanan tampilan, bukan lapisan
keamanan sesungguhnya.

---

## 4. Tiga Lapisan Data

| Lapisan | Isi |
|---|---|
| **Bronze** | Data upload yang sudah otomatis dibersihkan |
| **Silver** | Hasil pembersihan tambahan manual |
| **Gold** | Tidak disimpan — KPI/chart dihitung langsung di halaman Visualize |

Setiap upload ke format yang sudah dikenal otomatis digabung ke satu
dataset "Master" yang terus bertambah (kecuali "Other Format", yang selalu
jadi file Bronze tersendiri).

---

## 5. Menjalankan Tanpa Docker (Manual)

```bash
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt

# siapkan PostgreSQL & MinIO sendiri, lalu:
psql -h $PG_HOST -U $PG_USER -d $PG_DB -f db/schema.sql
python db/seed.py admin "password-kuat" "Workspace Admin"

python app.py   # http://localhost:5000, tanpa load balancer Nginx
```

---

## 6. Tips Development Lokal

`Dockerfile` meng-copy project saat build — folder project **bukan** live
volume di `docker-compose.yml`. Jadi:

- Edit file → **rebuild**: `docker compose up --build -d` (bukan sekadar
  `restart`).
- Kalau sering edit-edit, tambahkan bind mount `- .:/app` pada
  `x-app-common` di `docker-compose.yml` supaya perubahan langsung
  terlihat tanpa rebuild (cukup `docker compose restart app1 app2 app3`).
- Kalau perubahan tidak muncul di browser padahal sudah rebuild: coba hard
  refresh (Ctrl/Cmd+Shift+R) atau buka di jendela incognito — `script.js`
  dan `style.css` sering ter-cache browser.

---

## 7. Struktur Proyek

```
.
├── app.py                    # application factory: wiring services & routes
├── config.py                  # divisi, jenis dataset, kolom wajib, konfigurasi KPI/chart
├── requirements.txt
├── Dockerfile
├── docker-compose.yml         # postgres, minio, seed, app1-3, nginx
├── nginx.conf                  # load balancer di depan app1-3
├── .env                         # konfigurasi lokal
├── db/
│   ├── schema.sql               # diterapkan otomatis saat `docker compose up` pertama kali
│   ├── seed.py                   # membuat akun Administrator pertama
│   └── create_user.py            # membuat akun tambahan (Administrator/Employee)
├── routes/                       # satu file blueprint per topik
│   ├── _common.py                 # login_required / admin_required
│   ├── auth.py                     # login/session/logout
│   ├── datasets.py                 # katalog, preview, visualize, merge
│   ├── upload.py                    # validasi + upload
│   ├── etl.py                        # cleaning manual -> Silver
│   └── admin.py                       # edit/hapus baris, hapus dataset, audit log
├── services/                          # satu modul per sistem eksternal
│   ├── minio_service.py                 # object storage
│   ├── postgres_service.py               # database
│   ├── spark_service.py                   # pembersihan/merge/agregasi
│   ├── catalog_service.py                  # gabungan MinIO + metadata Postgres
│   └── rate_limiter.py                      # pembatas percobaan login
├── templates/index.html                    # single-page shell
└── static/
    ├── js/script.js                          # seluruh logika frontend
    └── css/style.css
```

---

## 8. Troubleshooting Singkat

- **Perubahan file tidak muncul** → pastikan sudah `docker compose up
  --build -d` (bukan hanya `restart`), lalu hard refresh browser.
- **`docker compose exec app ...` error "no such service"** → nama service
  yang benar adalah `app1`/`app2`/`app3`, bukan `app`.
- **Elemen di-set `.hidden = true` tapi tetap muncul** → kemungkinan ada
  CSS class (mis. `.btn`) yang mengatur `display` sendiri dan menimpa
  atribut `[hidden]`. Gunakan `element.remove()`, atau tambahkan
  `[hidden] { display: none !important; }`.

---

## 9. Hal yang Belum Selesai / Rencana Selanjutnya

- Belum ada UI untuk manajemen pengguna — masih lewat command line
  (`db/create_user.py`).
- Rate limiting login belum dibagi antar-container (perlu Redis,
  `REDIS_URL`) — saat ini tiap container app menghitung batasnya sendiri.
- Tabel `reporting_metrics` (untuk integrasi Tableau) sudah ada di schema,
  tapi belum ada proses yang mengisinya.
- Kartu "Active Jobs" di Dashboard masih selalu 0 — placeholder untuk fitur
  job monitor di masa depan.
