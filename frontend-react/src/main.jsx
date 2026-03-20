import React from "react";
import { createRoot } from "react-dom/client";
import { onCLS, onFCP, onINP, onLCP, onTTFB } from "web-vitals";
import App from "./App";
import "./styles.css";

createRoot(document.getElementById("root")).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
);

const apiBase = (import.meta.env.VITE_API_BASE_URL || "").replace(/\/$/, "");

function buildApiUrl(path) {
  return apiBase ? `${apiBase}${path}` : path;
}

function sendVital(metric) {
  const payload = {
    name: metric.name,
    value: metric.value,
    rating: metric.rating,
    id: metric.id,
    navigationType: metric.navigationType || "unknown",
  };
  const body = JSON.stringify(payload);
  const url = buildApiUrl("/api/frontend/vitals");
  if (navigator.sendBeacon) {
    const blob = new Blob([body], { type: "application/json" });
    navigator.sendBeacon(url, blob);
    return;
  }
  fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body,
    keepalive: true,
  }).catch(() => undefined);
}

onCLS(sendVital);
onFCP(sendVital);
onINP(sendVital);
onLCP(sendVital);
onTTFB(sendVital);
