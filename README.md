# ⚡ WebVideoCast Studio v3.0 (PC, Mobile & Smart TV)

Bộ công cụ phát video, danh sách nhạc/phim YouTube lặp lại, luồng HLS (`.m3u8`), MP4, phụ đề rời (`.srt` / `.vtt`) và file ổ cứng máy tính trực tiếp lên **Smart TV (LG, Samsung, Sony, TCL, Android TV...)** thông qua giao thức chuẩn `Web Video Caster Receiver` (`cast2tv.app`), kèm điều khiển đồng bộ thời gian thực giữa **Máy Tính (PC)** và **Điện Thoại (Mobile)**.

---

## ✨ Tính Năng Nổi Bật

- **🔴 Phòng Giải Trí YouTube & Danh Sách Phát Lặp Lại (Jukebox):**
  - Tìm kiếm trực tiếp video/nhạc/phim trên YouTube, xem thời lượng & ảnh đại diện rõ nét.
  - Tạo danh sách phát chung, hỗ trợ chế độ **Lặp lại cả danh sách (`Loop All`)**, **Lặp lại 1 bài (`Loop One`)**, **Tự động chuyển tập (`Auto-Next`)**, và lưu/nạp nhiều danh sách phát yêu thích.
  - Bộ đệm proxy phân mảnh (`Bounded Byte-Range Proxy`) vượt lỗi `403 Forbidden` của YouTube khi phát trên Smart TV.
- **🎬 Bóc Tách Phim Web (`.m3u8` / `.mp4`) & Tự Động Nhận Diện Danh Sách Tập:**
  - Tự động bóc tách luồng phim chính (1080p/720p), lọc bỏ quảng cáo/clip rác, tự động gọt vỏ header PNG/GIF/JPEG giả dạng trên các trang phim HLS và tự động chuyển đổi phụ đề `.srt` sang `WebVTT (.vtt)` chuẩn UTF-8.
  - Tự động nhận diện danh sách tập phim bộ trên nhiều cấu trúc website khác nhau và tự động chuyển tập khi hết phim.
- **🌐 Trình Duyệt WVC Nhúng & Tiện Ích Chrome Extension:**
  - Tích hợp trình duyệt chặn quảng cáo và tự động bắt luồng video/YouTube ngay khi bấm Play.
- **📱 Đồng Bộ Thời Gian Thực Giữa PC & Điện Thoại (Mobile Remote):**
  - Giao diện tự động tối ưu cho màn hình dọc điện thoại. Mọi danh sách phát, kết quả tìm kiếm và tiến độ phát (nhảy theo từng giây) đều đồng bộ tức thì giữa máy tính và điện thoại trong cùng mạng Wi-Fi.

---

## 🚀 Cài Đặt & Sử Dụng Nhanh (Windows)

1. **Cài đặt lần đầu:**
   - Chạy file **`1_CAI_DAT_LAN_DAU.bat`** để tự động cài đặt các thư viện trong `requirements.txt`, trình duyệt ngầm Chromium (`playwright`) và mở cổng mạng LAN (`30001`).
2. **Khởi động ứng dụng:**
   - Chạy file **`2_CHAY_WEBVIDEOCAST_STUDIO.bat`** (hoặc `python server.py`).
   - Mở trình duyệt trên PC tại: `http://localhost:30001`
   - Mở trên Điện Thoại (cùng Wi-Fi) theo địa chỉ IP LAN hiển thị trên thanh tiêu đề (ví dụ: `http://192.168.1.x:30001`).
3. **Kết nối với Smart TV:**
   - Mở ứng dụng **Web Video Caster Receiver** trên Smart TV (hoặc truy cập `http://cast2tv.app` trên trình duyệt TV) và nhập mã kết nối hiển thị trên màn hình điều khiển (TV sẽ tự động kết nối ngay trong mạng LAN).
