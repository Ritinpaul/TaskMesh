import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "TaskMesh | Distributed DAG Workflow Orchestration Engine",
  description: "High-Throughput Distributed Task Engine & DAG Workflow Orchestrator built on FastAPI and Redis Streams",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
