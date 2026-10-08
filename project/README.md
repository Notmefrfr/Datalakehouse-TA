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
│   ├── analysis_tools.py                    # 20 tool asisten AI (data, visualisasi, analisis, ML)
│   ├── chat_agent.py                         # loop tool-calling Qwen3 8B
│   ├── sql_sandbox.py                         # SELECT read-only (DuckDB) untuk tool run_query
│   └── rate_limiter.py                      # pembatas percobaan login
│   ├── delta_service.py                     # tabel Delta Lake untuk Master 
│   └── compaction_job.py                     # job background: kompaksi file kecil 
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
- **Upload file besar masih berisiko**: `MAX_CONTENT_LENGTH` sudah dinaikkan
  ke 10 GB, tapi `routes/upload.py` (`_read_upload_file()`) masih membaca
  seluruh file ke memori sekaligus (`file.read().decode("utf-8")`) sebelum
  validasi/parsing mulai. Menaikkan batas ini membuat server MAU menerima
  file 10 GB, tapi belum tentu BISA memprosesnya tanpa crash — perlu upload
  & parsing yang streaming (baca-validasi-tulis per potongan, tidak pernah
  menyimpan seluruh file di memori) sebelum benar-benar aman dicoba dengan
  file sebesar itu. Upload ke MinIO sendiri sudah multipart & paralel
  (`services/minio_service.py`, lihat `_UPLOAD_TRANSFER_CONFIG`) — ini
  bukan titik lemahnya lagi, titik lemahnya di tahap baca+validasi di Flask
  sebelum data itu sampai ke MinIO.
- **Batas konkurensi tool AI (`CHAT_MAX_CONCURRENT_TOOLS`) per proses, bukan
  per cluster** — dengan 3 replika app x beberapa worker, batas nyata di
  seluruh deployment adalah kelipatannya, bukan angka config itu sendiri.
  Koordinasi lintas-replika yang sebenarnya butuh sesuatu yang dibagi
  bersama (mis. Redis counter atau advisory lock Postgres) — belum ada.

## 10. Master Dataset: Parquet + Delta Lake

Dataset "Master" (yang otomatis di-merge tiap upload) sekarang disimpan
sebagai tabel **Delta Lake** (Parquet + transaction log) di MinIO lewat
`deltalake` (delta-rs) — bukan lagi satu file CSV raksasa yang ditulis
ulang penuh tiap kali ada upload.

- Tiap upload jadi satu file Parquet kecil sendiri (append biasa untuk
  mode "keep"/"append_raw", atau Delta MERGE beneran untuk
  "remove"/"replace").
- Job background (`services/compaction_job.py`) mengecek tiap
  `COMPACT_CHECK_INTERVAL_HOURS` jam, dan mengompres file-file kecil
  suatu format jadi lebih sedikit & lebih besar (target
  `COMPACT_TARGET_FILE_SIZE_MB`) kalau sudah lebih dari
  `COMPACT_INTERVAL_DAYS` sejak kompaksi terakhir.
- Data master lama (CSV, dari sebelum update ini) otomatis dipindahkan
  ke tabel Delta yang baru, satu kali, pertama kali format itu disentuh
  lagi — tidak perlu migrasi manual.
- Download & halaman Visualize tidak berubah sama sekali dari sisi
  pengguna: keduanya tetap melihat satu dataset gabungan per kategori,
  walau di baliknya sudah terpecah jadi banyak file Parquet.
- Belum ada tombol "compact sekarang" di UI admin — kompaksi hanya
  berjalan via job terjadwal di atas.

---

## 11. Asisten AI: Qwen3 8B + 20 Tool

Prinsip arsitektur: **model AI tidak pernah menerima atau memproses seluruh dataset.**
Qwen3 8B hanya menjadi antarmuka cerdas — memahami pertanyaan pengguna dan memilih
analisis yang dibutuhkan. Perhitungan sebenarnya dijalankan oleh tool di sisi server
(DuckDB/SQL, pandas, scikit-learn) pada data lengkap di lakehouse. Hanya hasil yang
relevan (statistik, agregat, metrik, titik grafik — biasanya < 4.500 karakter) yang
dikirim balik ke Qwen, yang kemudian menjelaskannya dan menampilkan grafik/heatmap.
Dengan begitu model lokal yang ringan dapat bekerja dengan dataset besar tanpa
melampaui context window.

| Kelompok | Tool |
|---|---|
| Data | `list_datasets`, `get_schema`, `get_sample`, `get_statistics`, `run_query` |
| Visualisasi | `bar_chart`, `line_chart`, `scatter_plot`, `heatmap`, `histogram`, `boxplot` |
| Analisis | `correlation`, `outlier_detection`, `missing_value_analysis`, `summary_statistics` |
| Machine learning | `decision_tree`, `KNN`, `linear_regression`, `logistic_regression`, `KMeans` |

Catatan penting:

- Semua tool **read-only**. `run_query` hanya menerima satu `SELECT` (lihat
  `services/sql_sandbox.py`); tool lain tidak punya jalur tulis ke storage.
- Perhitungan berjalan **di dalam proses Flask** (DuckDB + pandas + scikit-learn),
  bukan di cluster Spark. `SPARK_MODE=cluster` hanya memengaruhi ETL (upload/cleaning).
- Tool ML dilatih pada maksimal `ML_MAX_TRAIN_ROWS` baris (sampel acak jika lebih besar),
  dengan split uji 80/20; hasilnya selalu disertai baseline (mayoritas / rata-rata)
  sebagai pembanding. Pada dataset yang lebih besar dari sampel, tentukan sendiri
  apakah sampel itu cukup representatif.
- Setiap pemakaian tool dicatat di audit log: `ai_sql` untuk `run_query`, `ai_tool`
  untuk tool lainnya.
- Skema 20 tool memakan ~2.000 token konteks per permintaan. Naikkan `LLM_NUM_CTX`
  ke 16384 bila RAM/VRAM host Ollama mencukupi.
- Menambah `scikit-learn` ke `requirements.txt` berarti image Docker perlu di-build ulang
  (`docker compose build`).

### 11.1 Bagaimana tool menangani dataset besar

- **`run_query`** membaca dataset lewat `pyarrow.dataset` yang *lazy* (belum
  membaca baris apa pun) lalu diserahkan ke DuckDB, yang baru benar-benar
  men-scan file Parquet yang relevan saat query dijalankan — dengan column/
  predicate pushdown. Artinya `run_query` selalu memberi jawaban **eksak**
  dari keseluruhan tabel, dan tidak pernah memuat seluruh dataset ke RAM
  sekaligus — aman dipakai di dataset yang jauh lebih besar dari RAM worker.
- **19 tool lainnya** (schema/statistik/chart/analisis/ML) bekerja di atas
  **sampel acak maksimal `CHAT_SAMPLE_MAX_ROWS`** (default 500.000 baris),
  diambil lewat `USING SAMPLE ... ROWS` di DuckDB — bukan baca penuh lalu
  dipotong di pandas. Kalau dataset lebih besar dari batas ini, hasil tool
  tetap disertai `sample_note` ("berdasarkan sampel X dari Y baris") yang
  wajib disampaikan apa adanya oleh model, bukan ditulis seolah angka pasti.
  Pertanyaan yang butuh angka eksak (total/jumlah pasti) sebaiknya dijawab
  lewat `run_query`.
- Tool ML punya batas sampel keduanya: `CHAT_SAMPLE_MAX_ROWS` dulu, lalu
  `ML_MAX_TRAIN_ROWS` (default 50.000) di atas sampel itu untuk training.
  Catatan "dilatih dari sampel X dari Y baris" di hasil ML selalu memakai Y =
  jumlah baris **asli** dataset (dari metadata Parquet, bukan dari sampel
  yang sudah dipotong), supaya tidak salah lapor.
- Tiap pemanggilan tool dibatasi `CHAT_TOOL_TIMEOUT_SECONDS` (default 45
  detik) dan jumlah tool yang boleh jalan bersamaan dalam satu proses worker
  dibatasi `CHAT_MAX_CONCURRENT_TOOLS` (default 4). **Catatan jujur:** ini
  timeout "lunak" (Python tidak bisa benar-benar membunuh thread yang macet,
  sehingga pekerjaan di baliknya tetap jalan sampai selesai sendiri) dan
  batas konkurensinya **per proses**, bukan across 3 replika app sekaligus
  — lihat bagian 9.

### 11.2 Kebutuhan Hardware (CPU/GPU) untuk Ollama + Qwen3 8B

Angka di bawah ini perkiraan umum dari dokumentasi Ollama/llama.cpp untuk
model 8B kuantisasi `q4_K_M` (default `ollama pull qwen3:8b`) — **bukan hasil
benchmark di server kalian**. Sebelum uji big-data minggu depan, sebaiknya
diverifikasi langsung di mesin yang akan dipakai (lihat cara cek di bawah).

| | CPU-only | GPU (disarankan) |
|---|---|---|
| Ukuran model di disk | ~5 GB | ~5 GB |
| RAM/VRAM dipakai saat jalan (`LLM_NUM_CTX=8192`) | ~8–12 GB RAM | ~6–8 GB VRAM |
| RAM/VRAM dipakai saat jalan (`LLM_NUM_CTX=16384`) | ~10–14 GB RAM | ~8–10 GB VRAM |
| Kecepatan perkiraan | ~3–10 token/detik | ~30–80+ token/detik |
| GPU minimum disarankan | — | NVIDIA, 8 GB VRAM (mis. RTX 3060 12GB) |

Hal lain yang perlu diperhatikan:

- **Konkurensi di level LLM, bukan cuma di level tool.** Ollama secara
  default menjalankan request secara berurutan kalau VRAM/compute terbatas
  (`OLLAMA_NUM_PARALLEL` biasanya otomatis 1 pada GPU kecil atau CPU-only).
  Jadi walau `CHAT_MAX_CONCURRENT_TOOLS=4` membolehkan 4 tool jalan
  bersamaan, pertanyaan dari beberapa orang sekaligus ke asisten AI bisa
  tetap antre satu per satu di Ollama — terasa lambat, bukan error. Naikkan
  `OLLAMA_NUM_PARALLEL` kalau VRAM mencukupi untuk benar-benar paralel.
- **Context besar = lebih lambat & lebih besar kebutuhan memori**, karena
  KV-cache (memori kerja model untuk "mengingat" percakapan) berukuran
  proporsional dengan panjang context. Skema 20-tool makan ~2.000 token di
  awal setiap percakapan — pada dataset dengan banyak kolom, `get_schema`/
  `get_statistics` bisa menambah beberapa ribu token lagi.
- **Cara cek pemakaian nyata di mesin kalian:**
  ```bash
  # VRAM GPU yang sedang dipakai Ollama
  docker compose exec ollama nvidia-smi
  # Log Ollama (termasuk eval_duration/tokens-per-second per request)
  docker compose logs ollama --tail 50
  ```
- Kalau GPU tidak tersedia, pertimbangkan model yang lebih kecil
  (`qwen3:4b`) untuk uji coba awal — jauh lebih ringan, walau kemampuan
  tool-calling-nya sedikit lebih lemah dibanding versi 8B.
