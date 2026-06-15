import { initializeApp } from "https://www.gstatic.com/firebasejs/12.7.0/firebase-app.js";
import { collection, getFirestore, onSnapshot } from "https://www.gstatic.com/firebasejs/12.7.0/firebase-firestore.js";
import { firebaseConfig, liveSetupsCollection } from "./firebase-live-monitor-config.js";
import { dateOf } from "./firebase-live-monitor-card.js";
import { updateView } from "./firebase-live-monitor-view.js";

export function startFirebaseSource() {
  try {
    const app = initializeApp(firebaseConfig, "zyni-live-monitor");
    const db = getFirestore(app);

    onSnapshot(
      collection(db, liveSetupsCollection),
      (snapshot) => {
        const signals = snapshot.docs
          .map((doc) => ({ id: doc.id, ...doc.data() }))
          .sort((a, b) =>
            (dateOf(b.createdAt)?.getTime() || 0) -
            (dateOf(a.createdAt)?.getTime() || 0)
          );
        updateView(
          signals,
          "connected",
          "Firestore connected · updates appear without refresh"
        );
      },
      (error) => {
        updateView([], "error", `${error.code || "firestore_error"}: ${error.message}`);
        console.warn("[Firebase Live Monitor] listener failed", error);
      }
    );
  } catch (error) {
    updateView([], "error", error.message || String(error));
    console.warn("[Firebase Live Monitor] startup failed", error);
  }
}
