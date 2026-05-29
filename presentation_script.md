# Skrip Presentasi: Memperkenalkan Dunia TI ke Anak SMA
**Target Durasi: 10 - 12 Menit**
**Tema: IT is More Than Just Coding - Building the Future**

---

## 1. PEMBUKAAN: Mitos vs Fakta Anak TI (0:00 - 1:30)
**Halo semuanya! Apa kabar teman-teman SMA?**

Senang banget bisa hadir di sini. Kenalin, namaku [Nama Kamu], dari jurusan Teknologi Informasi. 

Sebelum kita mulai, aku mau tanya: Kalau kalian denger kata "Anak IT", apa sih yang muncul di pikiran?
*   Ngetik kode rumit di depan layar hitam?
*   Benerin laptop tetangga?
*   Atau orang yang jarang keluar kamar?

Nah, hari ini aku mau bongkar rahasianya. Inti dari TI itu bukan cuma soal ngoding, tapi soal **gimana cara kita pakai teknologi buat nyelesain masalah di dunia nyata.** Aku mau nunjukkin dua proyek yang pernah aku bikin supaya kalian dapet gambaran: *"Se-keren apa sih dunia TI itu?"*

---

## 2. PROYEK 1: GEMPARADAR - Menyelamatkan Nyawa dengan Data (1:30 - 5:30)

### A. Kenapa Proyek Ini Ada? (1 Menit)
Kita semua tau Indonesia itu sering banget gempa. Masalahnya, seringkali info gempa itu cuma berupa angka-angka yang ngebingungin di berita. Padahal, saat darurat, kita butuh info yang **cepet, gampang diliat, dan jelas.** 

### B. Gimana Cara Kerjanya? (2 Menit)
*Sambil demo Dashboard: http://localhost:5000*

Proyek pertama aku namanya **GempaRadar**. Di sini, aku bikin "robot digital" yang tugasnya:
1.  **Narik Data Otomatis**: Robot ini keliling ke situs-situs gempa dunia (seperti USGS di Amerika) buat ambil data terbaru setiap menit. Kita nggak perlu capek cek satu-situs secara manual.
2.  **Bikin Peta Interaktif**: Data yang tadinya cuma angka koordinat, diubah sama sistem ini jadi titik-titik di peta. Jadi kalian bisa langsung liat: "Oh, gempanya di sini, kekuatannya segini."
3.  **Cari Berita Terkait**: Sistem ini juga otomatis cari berita di internet soal gempa itu. Jadi kalau ada kerusakan, kita bisa langsung tau kabarnya dari media online.

### C. Pesan buat Teman-teman (1 Menit)
Di sini kalian belajar bahwa di TI, kita bisa bikin alat yang membantu orang banyak buat lebih siap ngadepin bencana. Data yang tadinya "mati", jadi "hidup" dan bermanfaat di tangan kita.

---

## 3. TRANSISI: Menuju Kota Masa Depan (5:30 - 6:00)
Tadi kan kita bahas soal alam. Sekarang, gimana kalau teknologi itu kita pakai buat bikin kota tempat kita tinggal jadi **Pinter**? Seperti di film-film *Sci-Fi*, di mana semuanya serba otomatis.

---

## 4. PROYEK 2: NOVAPULSE - Membangun Otak Sebuah Kota (6:00 - 10:00)

### A. Bayangin Kota yang Bisa "Mikir" (1 Menit)
Pernah nggak kalian liat ambulans kejebak macet? Atau lampu merah yang lamanya minta ampun padahal jalanan lagi sepi? Itu terjadi karena sistem kota kita sekarang masih "jalan sendiri-sendiri". 

Proyek kedua aku namanya **NovaPulse**. Aku pengen bikin kota yang punya "Sistem Saraf". Jadi, semua bagian kota itu saling terhubung.

### B. Rahasia di Balik Layar: gRPC (1 Menit)
Di sini aku pakai teknologi namanya **gRPC**. Anggap aja ini adalah "jalur komunikasi super cepat". Kalau WhatsApp butuh waktu buat kirim pesan, gRPC ini kirim datanya secepat kilat (milidetik!). Kenapa harus cepet? Karena kalau soal nyawa atau darurat, kita nggak boleh nunggu.

### C. Demo: Keajaiban Otomasi (2 Menit)
*Sambil demo Dashboard: http://localhost:3020*

Coba liat di layar ini. Ada layanan Jalan Raya, Lingkungan, dan Darurat.
*   **Kejadian Otomatis**: Kalau ada kecelakaan dilaporin di jalan (Traffic), sistem NovaPulse bakal langsung kasih tau Rumah Sakit buat kirim ambulans **tanpa ada yang perlu telepon.**
*   **Respon Polusi**: Kalau sensor udara bilang "Polusinya bahaya nih!", lampu lalu lintas bisa otomatis disetel buat ngurangin kendaraan yang lewat di sana.

---

## 5. PROYEK 3: MINI SOC - Menjadi "Cyber Guard" Masa Depan (10:00 - 14:00)

### A. Kenapa Kita Butuh Benteng Digital? (1 Menit)
Pernah denger berita soal kebocoran data atau situs yang tiba-tiba nggak bisa dibuka karena serangan hacker? Di dunia digital, serangan itu terjadi setiap detik. Tanpa sistem pertahanan, data pribadi kita—seperti chat, foto, atau saldo bank—bisa hilang begitu saja.

### B. Gimana Cara Kerjanya? (2 Menit)
*Sambil demo Grafana: http://localhost:3000*

Proyek ketiga aku adalah **Mini SOC**. Ini adalah "Menara Pengawas" keamanan digital. Di sini aku pakai **AI (Artificial Intelligence)** buat jadi penjaganya:
1.  **Deteksi Real-time**: Sistem ini dengerin semua "bisikan" di jaringan komputer. Kalau ada aktivitas aneh, misalnya ada yang coba masuk paksa (brute force), sistem bakal langsung bunyiin alarm.
2.  **AI Analysis**: AI-nya bisa tau ini serangan jenis apa (misalnya SQL Injection atau DoS) dan langsung kasih tau cara memperbaikinya sebelum hacker-nya berhasil masuk.
3.  **Visualisasi Data Masif**: Semua serangan yang ribuan jumlahnya itu diubah jadi grafik yang keren di Grafana, jadi tim keamanan bisa liat kondisi "perang" digital secara jelas.

### C. Pesan buat Teman-teman (1 Menit)
Di TI, kalian bisa jadi "Digital Superhero" yang jagain data jutaan orang. Ini bukan cuma soal nangkep penjahat, tapi soal mastiin teknologi yang kita pakai sehari-hari itu aman.

---

## 6. PENUTUP & DISKUSI: Masa Depanmu di Dunia TI (14:00 - 19:00)
**Durasi: 5 Menit**

### A. Kesimpulan 3 Proyek (1 Menit)
Tadi kita udah liat:
1.  **GempaRadar**: Pakai data buat nyelamatin orang dari bencana.
2.  **NovaPulse**: Bikin kota jadi pinter dan serba otomatis.
3.  **Mini SOC**: Jadi garda terdepan keamanan dunia digital.

Ketiganya punya satu kesamaan: **Semuanya ngebangun solusi pakai teknologi.**

### B. "Why TI?" - Deep Dive (1.5 Menit)
Temen-temen SMA, TI itu bukan cuma buat orang yang jago matematika. TI itu buat orang yang **punya rasa penasaran tinggi.** 
*   Kalian suka main game? Di TI kalian bisa bikin dunianya.
*   Kalian suka bantuin orang? Di TI kalian bisa bikin alat yang nyelamatin nyawa.
*   Kalian pengen punya karir yang dicari semua perusahaan di dunia? TI jawabannya.

### C. Tanya Jawab & Interaksi (2 Menit)
*Ajak penonton bertanya: "Siapa di sini yang pernah kepikiran mau bikin aplikasi sendiri? Atau ada yang bingung gimana cara mulai belajar keamanan siber?"*
(Jawab pertanyaan dengan antusias, kaitkan dengan demo yang tadi dilakukan).

### D. Final Statement (0.5 Menit)
Dunia masa depan itu nggak cuma butuh pengguna teknologi, tapi butuh **Pencipta Teknologi**. Kalau kalian pengen jadi orang yang ngebangun masa depan, bukan cuma penonton, yuk gabung di jurusan TI!

**Terima kasih semuanya! Sampai ketemu di lab komputer!**

---

> [!TIP]
> **Tips buat kamu:**
> *   Pas bagian **Mini SOC**, tunjukin grafik serangan yang banyak dan bilang: "Bayangin kalau ini nggak kita jaga, data kita semua bisa hilang."
> *   Di bagian **Penutup 5 menit**, pakai 2 menit terakhir buat Q&A yang bener-bener interaktif. Jangan cuma nunggu mereka tanya, pancing mereka!
