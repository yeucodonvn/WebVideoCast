# ⚡ WebVideoCast Studio v3.0 (PC, Mobile & Smart TV)

Bộ công cụ phát video, danh sách nhạc/phim YouTube lặp lại, luồng HLS (`.m3u8`), MP4, phụ đề rời (`.srt` / `.vtt`) và file ổ cứng máy tính trực tiếp lên **Smart TV (LG, Samsung, Sony, TCL, Android TV...)** thông qua giao thức chuẩn `Web Video Caster Receiver` (`cast2tv.app`), kèm điều khiển đồng bộ thời gian thực giữa **Máy Tính (PC)** và **Điện Thoại (Mobile)**.

---

## 📥 Tải Về Bản Chạy Ngay Không Cần Cài Đặt (`.EXE` Standalone)

👉 **[Tải `WebVideoCast_Studio_v3.0.exe` tại trang Releases](https://github.com/yeucodonvn/WebVideoCast/releases/tag/v3.0.0)**

- **Không cần cài Python, không cần cài Node.js, không cần chạy lệnh cài đặt!**
- Chỉ cần tải duy nhất file **`WebVideoCast_Studio_v3.0.exe`** về máy tính Windows và **bấm đúp (Double-Click)** để chạy.
- Ứng dụng sẽ tự động mở bảng điều khiển tại `http://localhost:30001` và sẵn sàng kết nối với Smart TV cũng như Điện Thoại trong cùng mạng Wi-Fi!

---

## ✨ Tính Năng Nổi Bật

- **🔴 Phòng Giải Trí YouTube & Danh Sách Phát Lặp Lại (Jukebox):**
  - Tìm kiếm trực tiếp video/nhạc/phim trên YouTube, xem thời lượng & ảnh đại diện rõ nét.
  - Tạo danh sách phát chung, hỗ trợ chế độ **Lặp lại cả danh sách (`Loop All`)**, **Lặp lại 1 bài (`Loop One`)**, **Tự động chuyển tập (`Auto-Next`)**, và lưu/nạp nhiều danh sách phát yêu thích.
  - Bộ đệm proxy phân mảnh (`Bounded Byte-Range Proxy`) kèm nhân giải mã `Node.js` tích hợp sẵn bên trong `.exe` giúp vượt lỗi `403 Forbidden` của YouTube khi phát trên Smart TV.
- **🎬 Bóc Tách Phim Web (`.m3u8` / `.mp4`) & Tự Động Nhận Diện Danh Sách Tập:**
  - Tự động bóc tách luồng phim chính (1080p/720p), lọc bỏ quảng cáo/clip rác, tự động gọt vỏ header PNG/GIF/JPEG giả dạng trên các trang phim HLS và tự động chuyển đổi phụ đề `.srt` sang `WebVTT (.vtt)` chuẩn UTF-8.
  - Tự động nhận diện danh sách tập phim bộ trên nhiều cấu trúc website khác nhau và tự động chuyển tập khi hết phim.
- **🌐 Trình Duyệt WVC Nhúng & Tiện Ích Chrome Extension:**
  - Tự động sử dụng trình duyệt Chrome / Microsoft Edge có sẵn trên Windows để mở cửa sổ duyệt web chặn quảng cáo và tự động bắt luồng video/YouTube ngay khi bấm Play.
- **📱 Đồng Bộ Thời Gian Thực Giữa PC & Điện Thoại (Mobile Remote):**
  - Giao diện tự động tối ưu cho màn hình dọc điện thoại. Mọi danh sách phát, kết quả tìm kiếm và tiến độ phát (nhảy theo từng giây) đều đồng bộ tức thì giữa máy tính và điện thoại trong cùng mạng Wi-Fi.

---

## 🛠️ Chạy Từ Mã Nguồn Python (Dành Cho Lập Trình Viên)

1. Chạy file **`1_CAI_DAT_LAN_DAU.bat`** để tự động cài đặt các thư viện trong `requirements.txt`.
2. Chạy file **`2_CHAY_WEBVIDEOCAST_STUDIO.bat`** (hoặc `python server.py`).
