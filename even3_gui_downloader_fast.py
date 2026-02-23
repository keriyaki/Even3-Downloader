import threading
import time
import re
import csv
import urllib.parse
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

import tkinter as tk
from tkinter import ttk, filedialog, messagebox

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter


PDF_HREF_RE = re.compile(
    r"https?://static\.even3\.com/anais/\d+\.pdf(\?[^\"'>\s]+)?", re.I)


@dataclass
class WorkItem:
    work_id: str
    title: str
    work_url: str
    pdf_url: str
    filename: str


def normalize_anais_url(url: str) -> str:
    url = (url or "").strip()
    if not url:
        return ""
    if not url.endswith("/"):
        url += "/"
    return url


def extract_slug(anais_url: str) -> str:
    path = urlparse(anais_url).path.strip("/")
    parts = path.split("/")
    if len(parts) >= 2 and parts[0].lower() == "anais":
        return parts[1].lower()
    raise ValueError(
        "URL inválida. Ex: https://www.even3.com.br/anais/ennepe2022/")


def safe_filename(s: str, max_len: int = 140) -> str:
    s = re.sub(r"[\\/*?\"<>|:]+", "_", s)
    s = re.sub(r"\s+", " ", s).strip()
    if not s:
        s = "arquivo"
    if len(s) > max_len:
        s = s[:max_len].rstrip()
    return s


def guess_title_from_work_url(work_url: str) -> str:
    """
    Usa o pedaço depois do ID na própria URL pra criar um título sem precisar abrir a página.
    Ex: .../528929-ELES-NAO-TEM... -> "ELES NAO TEM ... "
    """
    path = urlparse(work_url).path.strip("/")
    last = path.split("/")[-1]  # "528929-AAA-BBB"
    m = re.match(r"(\d{3,})[-/](.+)$", last)
    if not m:
        return ""
    raw = m.group(2)
    raw = urllib.parse.unquote(raw)
    # limpa hifens e duplos
    raw = raw.replace("--", " - ")
    raw = raw.replace("-", " ")
    raw = re.sub(r"\s+", " ", raw).strip()
    return raw


def collect_work_urls_with_playwright(anais_url: str, slug: str, log_fn, progress_fn) -> list[str]:
    work_url_re = re.compile(
        rf"^https?://www\.even3\.com\.br/anais/{re.escape(slug)}/\d{{3,}}", re.I)
    collected = set()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/121.0.0.0 Safari/537.36"
            )
        )

        log_fn("Abrindo página do anais…")
        page.goto(anais_url, wait_until="networkidle", timeout=90_000)
        page.wait_for_timeout(1000)

        def scrape_links():
            try:
                hrefs = page.eval_on_selector_all(
                    "a[href]", "els => els.map(a => a.href)")
            except Exception:
                return 0
            new_count = 0
            for h in hrefs:
                if isinstance(h, str) and work_url_re.match(h):
                    clean = h.split("?")[0]
                    if clean not in collected:
                        collected.add(clean)
                        new_count += 1
            return new_count

        def click_next() -> bool:
            candidates = [
                'a:has-text("Próximo")',
                'button:has-text("Próximo")',
                'a:has-text("Next")',
                'button:has-text("Next")',
            ]
            for sel in candidates:
                loc = page.locator(sel).first
                try:
                    if loc.count() == 0 or not loc.is_visible():
                        continue
                    aria_disabled = (loc.get_attribute(
                        "aria-disabled") or "").lower()
                    cls = (loc.get_attribute("class") or "").lower()
                    if aria_disabled == "true" or "disabled" in cls:
                        return False

                    loc.click(timeout=5_000)
                    page.wait_for_load_state("networkidle", timeout=30_000)
                    page.wait_for_timeout(700)
                    return True
                except PlaywrightTimeoutError:
                    continue
                except Exception:
                    continue
            return False

        page_idx = 1
        stalled = 0

        while True:
            new_here = scrape_links()
            progress_fn(
                f"Coletando links… pág {page_idx} (+{new_here}), total {len(collected)}")
            if new_here == 0:
                stalled += 1
            else:
                stalled = 0
            if stalled >= 5:
                break
            if not click_next():
                break
            page_idx += 1

        browser.close()

    return sorted(collected)


def make_session(pool_size: int) -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121 Safari/537.36",
        "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
    })
    adapter = HTTPAdapter(pool_connections=pool_size,
                          pool_maxsize=pool_size, max_retries=2)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s


_thread_local = threading.local()


def get_thread_session(pool_size: int) -> requests.Session:
    if not hasattr(_thread_local, "session"):
        _thread_local.session = make_session(pool_size)
    return _thread_local.session


def parse_work_page_for_pdf(session: requests.Session, work_url: str) -> str | None:
    r = session.get(work_url, timeout=60)
    if r.status_code != 200:
        return None
    html = r.text
    m = PDF_HREF_RE.search(html)
    if m:
        return m.group(0)

    soup = BeautifulSoup(html, "html.parser")
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if PDF_HREF_RE.match(href):
            return href
    return None


def download_pdf(session: requests.Session, pdf_url: str, out_path: Path, delay: float) -> None:
    if delay > 0:
        time.sleep(delay)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_suffix(out_path.suffix + ".part")

    with session.get(pdf_url, stream=True, timeout=120, allow_redirects=True) as r:
        r.raise_for_status()
        with open(tmp, "wb") as f:
            # chunk maior = menos overhead
            for chunk in r.iter_content(chunk_size=1024 * 512):
                if chunk:
                    f.write(chunk)

    if tmp.stat().st_size < 1024:
        tmp.unlink(missing_ok=True)
        raise RuntimeError("Arquivo muito pequeno (provável erro/HTML).")

    tmp.replace(out_path)


def work_id_from_url(work_url: str) -> str:
    m = re.search(r"/anais/[^/]+/(\d{3,})", work_url, re.I)
    return m.group(1) if m else ""


def job_download(work_url: str, out_dir: Path, pool_size: int, delay: float) -> tuple[str, str, str, str, str]:
    """
    Retorna: (work_url, work_id, pdf_url, file_path, status)
    """
    session = get_thread_session(pool_size)

    wid = work_id_from_url(work_url)
    title = guess_title_from_work_url(work_url)
    base = wid if wid else "trabalho"
    if title:
        base = f"{base} - {title}"
    filename = safe_filename(base) + ".pdf"
    file_path = out_dir / filename

    if file_path.exists() and file_path.stat().st_size > 0:
        return (work_url, wid, "", str(file_path), "exists")

    # 1) tentativa rápida: baixar direto por ID.pdf
    direct_pdf = f"https://static.even3.com/anais/{wid}.pdf" if wid else ""
    if direct_pdf:
        try:
            download_pdf(session, direct_pdf, file_path, delay=delay)
            return (work_url, wid, direct_pdf, str(file_path), "downloaded_direct")
        except Exception:
            # fallback abaixo
            pass

    # 2) fallback: abre página do trabalho e pega o PDF “real”
    pdf_url = parse_work_page_for_pdf(session, work_url)
    if not pdf_url:
        return (work_url, wid, "", "", "no_pdf")

    try:
        download_pdf(session, pdf_url, file_path, delay=delay)
        return (work_url, wid, pdf_url, str(file_path), "downloaded_fallback")
    except Exception:
        return (work_url, wid, pdf_url, "", "error")


class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Even3 – Baixar PDFs dos Anais (Turbo)")
        self.geometry("900x600")

        self.url_var = tk.StringVar(
            value="https://www.even3.com.br/anais/ennepe2022/")
        self.out_var = tk.StringVar(
            value=str((Path.cwd() / "even3_downloads").resolve()))
        # <<< aumenta aqui se quiser (8~16 costuma ser bom)
        self.workers_var = tk.IntVar(value=10)
        self.delay_var = tk.DoubleVar(value=0.0)   # <<< 0.0 = mais rápido

        self.running = False
        self._build_ui()

    def _build_ui(self):
        frm = ttk.Frame(self, padding=12)
        frm.pack(fill="both", expand=True)

        ttk.Label(frm, text="Link do anais (Even3):").grid(
            row=0, column=0, sticky="w")
        ttk.Entry(frm, textvariable=self.url_var).grid(
            row=1, column=0, columnspan=3, sticky="ew", pady=(4, 12))

        ttk.Label(frm, text="Pasta de saída:").grid(
            row=2, column=0, sticky="w")
        ttk.Entry(frm, textvariable=self.out_var).grid(
            row=3, column=0, columnspan=2, sticky="ew", pady=(4, 12))
        ttk.Button(frm, text="Escolher…", command=self.pick_folder).grid(
            row=3, column=2, sticky="ew", padx=(10, 0))

        # controles de performance
        perf = ttk.Frame(frm)
        perf.grid(row=4, column=0, columnspan=3, sticky="ew", pady=(0, 10))
        ttk.Label(perf, text="Workers (paralelo):").grid(
            row=0, column=0, sticky="w")
        ttk.Spinbox(perf, from_=1, to=32, textvariable=self.workers_var, width=6).grid(
            row=0, column=1, sticky="w", padx=(6, 14))
        ttk.Label(perf, text="Delay por request (s):").grid(
            row=0, column=2, sticky="w")
        ttk.Spinbox(perf, from_=0.0, to=2.0, increment=0.05, textvariable=self.delay_var, width=6).grid(
            row=0, column=3, sticky="w", padx=(6, 0))
        perf.columnconfigure(4, weight=1)

        self.btn_start = ttk.Button(
            frm, text="Baixar PDFs", command=self.start)
        self.btn_start.grid(row=5, column=0, sticky="ew")
        self.btn_stop = ttk.Button(
            frm, text="Parar", command=self.stop, state="disabled")
        self.btn_stop.grid(row=5, column=1, sticky="ew", padx=(10, 0))

        self.status_var = tk.StringVar(value="Pronto.")
        ttk.Label(frm, textvariable=self.status_var).grid(
            row=6, column=0, columnspan=3, sticky="w", pady=(12, 6))

        self.pbar = ttk.Progressbar(frm, mode="determinate")
        self.pbar.grid(row=7, column=0, columnspan=3, sticky="ew")

        ttk.Label(frm, text="Log:").grid(
            row=8, column=0, sticky="w", pady=(12, 0))
        self.log = tk.Text(frm, height=18, wrap="word")
        self.log.grid(row=9, column=0, columnspan=3,
                      sticky="nsew", pady=(4, 0))

        frm.columnconfigure(0, weight=1)
        frm.rowconfigure(9, weight=1)

    def pick_folder(self):
        path = filedialog.askdirectory()
        if path:
            self.out_var.set(path)

    def log_line(self, msg: str):
        def _append():
            self.log.insert("end", msg + "\n")
            self.log.see("end")
        self.after(0, _append)

    def set_status(self, msg: str):
        self.after(0, lambda: self.status_var.set(msg))

    def stop(self):
        self.running = False
        self.set_status("Parando… (aguarde)")
        self.log_line("Pedido de parada recebido. Finalizando…")

    def start(self):
        if self.running:
            return

        anais_url = normalize_anais_url(self.url_var.get())
        out_dir = Path(self.out_var.get()).expanduser().resolve()
        workers = int(self.workers_var.get())
        delay = float(self.delay_var.get())

        if not anais_url:
            messagebox.showerror("Erro", "Cole o link do anais.")
            return

        try:
            slug = extract_slug(anais_url)
        except Exception as e:
            messagebox.showerror("Erro", str(e))
            return

        out_dir.mkdir(parents=True, exist_ok=True)

        self.running = True
        self.btn_start.configure(state="disabled")
        self.btn_stop.configure(state="normal")
        self.pbar["value"] = 0
        self.pbar["maximum"] = 100

        t = threading.Thread(
            target=self.worker,
            args=(anais_url, slug, out_dir, workers, delay),
            daemon=True
        )
        t.start()

    def worker(self, anais_url: str, slug: str, out_dir: Path, workers: int, delay: float):
        try:
            self.log_line(f"Anais: {anais_url}")
            self.log_line(f"Slug: {slug}")
            self.log_line(f"Saída: {out_dir}")
            self.log_line(f"Workers: {workers} | Delay: {delay}s")

            def progress_text(msg: str):
                self.set_status(msg)

            # 1) coletar URLs de trabalhos (dinâmico)
            work_urls = collect_work_urls_with_playwright(
                anais_url, slug, self.log_line, progress_text)
            if not work_urls:
                raise RuntimeError(
                    "Não encontrei links de trabalhos. Talvez o anais esteja com layout diferente.")

            if not self.running:
                return

            total = len(work_urls)
            self.log_line(f"Total de trabalhos encontrados: {total}")
            self.set_status("Baixando PDFs em paralelo…")

            self.after(0, lambda: self.pbar.config(maximum=total))
            self.after(0, lambda: self.pbar.config(value=0))

            manifest = out_dir / "manifest.csv"
            ok = 0
            no_pdf = 0
            err = 0

            pool_size = max(8, workers * 2)

            with open(manifest, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(
                    ["work_url", "work_id", "pdf_url", "file_path", "status"])

                with ThreadPoolExecutor(max_workers=workers) as ex:
                    futures = [
                        ex.submit(job_download, u, out_dir, pool_size, delay) for u in work_urls]

                    done_count = 0
                    for fut in as_completed(futures):
                        if not self.running:
                            break

                        work_url, wid, pdf_url, file_path, status = fut.result()
                        w.writerow([work_url, wid, pdf_url, file_path, status])

                        done_count += 1
                        self.after(
                            0, lambda v=done_count: self.pbar.config(value=v))
                        self.set_status(f"Concluídos {done_count}/{total}…")

                        if status.startswith("downloaded") or status == "exists":
                            ok += 1
                        elif status == "no_pdf":
                            no_pdf += 1
                        else:
                            err += 1

            if self.running:
                self.set_status(
                    f"Concluído! OK/exists: {ok} | Sem PDF: {no_pdf} | Erros: {err}")
                self.log_line(
                    f"Concluído! OK/exists: {ok} | Sem PDF: {no_pdf} | Erros: {err}")
                self.log_line(f"Manifest: {manifest}")

        except Exception as e:
            messagebox.showerror("Erro", str(e))
            self.log_line(f"ERRO: {repr(e)}")
            self.set_status("Erro.")
        finally:
            self.running = False
            self.after(0, lambda: self.btn_start.config(state="normal"))
            self.after(0, lambda: self.btn_stop.config(state="disabled"))


if __name__ == "__main__":
    App().mainloop()
