import { redirect } from "next/navigation";

export default function Home({ params }: { params: Promise<{ locale: string }> }) {
  redirect("/jobs");
}
