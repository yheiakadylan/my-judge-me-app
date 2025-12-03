// api/ingest.js
import { db, admin } from '../lib/firebase.js';

const INGEST_SECRET = "changeme-very-secret"; // Đổi giống client
const REVIEWS_PER_DOC = 200;

export default async function handler(req, res) {
  // 1. Setup CORS
  res.setHeader('Access-Control-Allow-Credentials', true);
  res.setHeader('Access-Control-Allow-Origin', '*'); // Hoặc domain cụ thể của bạn
  res.setHeader('Access-Control-Allow-Methods', 'GET,OPTIONS,PATCH,DELETE,POST,PUT');
  res.setHeader(
    'Access-Control-Allow-Headers',
    'X-CSRF-Token, X-Requested-With, Accept, Accept-Version, Content-Length, Content-MD5, Content-Type, Date, X-Api-Version'
  );

  if (req.method === 'OPTIONS') {
    res.status(200).end();
    return;
  }

  if (req.method !== 'POST') {
    return res.status(405).json({ ok: false, error: 'Method Not Allowed' });
  }

  try {
    const { secret, page, reviews, totalGuess, done } = req.body;

    // 2. Bảo mật
    if (secret !== INGEST_SECRET) {
      return res.status(401).json({ ok: false, error: "Invalid secret" });
    }

    // 3. Xử lý lưu Reviews (Chunking)
    if (Array.isArray(reviews) && reviews.length > 0 && page > 0) {
      await db.collection("jdgm_pages").doc(`p_${page}`).set(
        {
          page,
          reviews, // Lưu mảng reviews vào
          count: reviews.length // Lưu số lượng để dễ tính sau này
        },
        { merge: true }
      );
      return res.json({ ok: true, action: "saved", page, count: reviews.length });
    }

    // 4. Xử lý tính toán AVG khi DONE = true
    // Logic này thay thế cho hàm aggregateExactAuto ở client cũ
    if (done === true) {
      // Lấy tất cả các pages đã lưu
      const pagesSnap = await db.collection("jdgm_pages").get();
      
      let sumRating = 0;
      let totalReviews = 0;
      let hist = { 1: 0, 2: 0, 3: 0, 4: 0, 5: 0 };

      // Duyệt qua từng doc (p_1, p_2...) để tính toán
      pagesSnap.docs.forEach(doc => {
        const data = doc.data();
        const list = data.reviews || [];
        
        list.forEach(r => {
          const rating = Number(r.rating || r.score || 0);
          if (rating >= 1 && rating <= 5) {
            sumRating += rating;
            totalReviews++;
            hist[rating]++;
          }
        });
      });

      const avg = totalReviews > 0 ? (sumRating / totalReviews) : 0;

      // Lưu kết quả meta
      await db.collection("jdgm_meta").doc("meta").set({
        total: totalReviews,
        avg: Number(avg.toFixed(4)), // Làm tròn 4 số
        hist,
        pagesCount: pagesSnap.size,
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        lastTotalGuess: totalGuess || 0
      }, { merge: true });

      return res.json({ 
        ok: true, 
        action: "aggregated", 
        stats: { total: totalReviews, avg, hist } 
      });
    }

    return res.json({ ok: true, note: "Nothing to do" });

  } catch (e) {
    console.error(e);
    return res.status(500).json({ ok: false, error: e.message });
  }
}