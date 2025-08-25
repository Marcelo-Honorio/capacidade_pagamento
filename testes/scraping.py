import time
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import ElementClickInterceptedException, TimeoutException

URL = "https://portaldeinformacoes.conab.gov.br/precos-agropecuarios-serie-historica.html"
DOWNLOAD_DIR = Path.cwd() / "downloads_conab"
DOWNLOAD_DIR.mkdir(exist_ok=True)

def make_driver(headless=False):
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--window-size=1600,1000")
    prefs = {
        "download.default_directory": str(DOWNLOAD_DIR.resolve()),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    }
    opts.add_experimental_option("prefs", prefs)
    return webdriver.Chrome(options=opts)

def close_cookies(driver):
    for xp in [
        "//*[@id='onetrust-accept-btn-handler']",
        "//button[contains(.,'Aceitar')]",
        "//button[contains(.,'Concordo')]",
        "//button[normalize-space()='OK']",
    ]:
        try:
            WebDriverWait(driver, 4).until(EC.element_to_be_clickable((By.XPATH, xp))).click()
            time.sleep(0.3); break
        except Exception:
            pass

def goto_pentaho_iframe(driver):
    WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
    time.sleep(1)
    iframes = driver.find_elements(By.TAG_NAME, "iframe")
    # prioriza os que parecem Pentaho
    for f in iframes:
        src = (f.get_attribute("src") or "").lower()
        if any(s in src for s in ["pentaho", "generatedcontent", "plugin"]):
            driver.switch_to.frame(f)
            return True
    if iframes:
        driver.switch_to.frame(iframes[0]); return True
    return False

def select_by_label(driver, label_text, option_text, timeout=12):
    holder = WebDriverWait(driver, timeout).until(EC.presence_of_element_located((
        By.XPATH,
        f"//label[normalize-space()='{label_text}']/following::*[(self::select or self::div) and not(self::label)][1]"
    )))
    if holder.tag_name.lower() == "select":
        Select(holder).select_by_visible_text(option_text)
        # força evento change (alguns paineis precisam)
        driver.execute_script("arguments[0].dispatchEvent(new Event('change', {bubbles:true}));", holder)
        return True
    # dropdown custom
    try:
        holder.click()
    except ElementClickInterceptedException:
        driver.execute_script("arguments[0].click();", holder)
    opt = WebDriverWait(driver, timeout).until(EC.element_to_be_clickable((
        By.XPATH,
        f"//li[normalize-space()='{option_text}'] | "
        f"//div[@role='option' or @role='listitem'][normalize-space()='{option_text}'] | "
        f"//*[self::span or self::div][normalize-space()='{option_text}']"
    )))
    opt.click()
    return True

def pick_min_max_period(driver):
    # tenta pares comuns de labels
    for ini, fim in [("Período Inicial","Período Final"), ("Data Inicial","Data Final"), ("Início","Fim")]:
        try:
            s_ini = WebDriverWait(driver, 6).until(EC.presence_of_element_located((By.XPATH, f"//label[normalize-space()='{ini}']/following::select[1]")))
            s_fim = WebDriverWait(driver, 6).until(EC.presence_of_element_located((By.XPATH, f"//label[normalize-space()='{fim}']/following::select[1]")))
            S0, S1 = Select(s_ini), Select(s_fim)
            S0.select_by_index(0)
            driver.execute_script("arguments[0].dispatchEvent(new Event('change', {bubbles:true}));", s_ini)
            S1.select_by_index(len(S1.options)-1)
            driver.execute_script("arguments[0].dispatchEvent(new Event('change', {bubbles:true}));", s_fim)
            return True
        except Exception:
            continue
    # fallback: dois selects após texto 'Período'
    try:
        near = driver.find_elements(By.XPATH, "(//*[contains(normalize-space(),'Período')])[1]/following::select[position()<=2]")
        if len(near) >= 2:
            S0, S1 = Select(near[0]), Select(near[1])
            S0.select_by_index(0); driver.execute_script("arguments[0].dispatchEvent(new Event('change',{bubbles:true}));", near[0])
            S1.select_by_index(len(S1.options)-1); driver.execute_script("arguments[0].dispatchEvent(new Event('change',{bubbles:true}));", near[1])
            return True
    except Exception:
        pass
    return False

def click_search_if_any(driver):
    for t in ["Pesquisar","Buscar","Aplicar","Atualizar","Filtrar","OK"]:
        try:
            btn = WebDriverWait(driver, 3).until(EC.element_to_be_clickable((By.XPATH, f"//button[normalize-space()='{t}']")))
            btn.click(); return True
        except Exception:
            continue
    return False

def wait_render(driver, timeout=60):
    end = time.time() + timeout
    while time.time() < end:
        # qualquer evidência de renderização
        for xp in [
            "//table[.//tr]",
            "//*[name()='svg' or self::canvas]",
            "//div[contains(@class,'exportElement') and contains(normalize-space(),'Dados')]",
        ]:
            try:
                el = driver.find_element(By.XPATH, xp)
                if el.is_displayed():
                    return True
            except Exception:
                pass
        time.sleep(1)
    raise TimeoutException("Renderização não detectada a tempo.")

def click_export_dados(driver, timeout=20):
    # seu botão: <div class="exportElement">Dados</div>
    exp = WebDriverWait(driver, timeout).until(EC.element_to_be_clickable((
        By.CSS_SELECTOR, "div.exportElement"
    )))
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", exp)
    time.sleep(0.2)
    try:
        exp.click()
    except ElementClickInterceptedException:
        driver.execute_script("arguments[0].click();", exp)

    # se abrir menu de formatos, clique CSV/Excel; senão, aguarde download direto
    for fmt in ["CSV","Excel","XLSX","XLS"]:
        try:
            opt = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((
                By.XPATH, f"//*/li[normalize-space()='{fmt}'] | //button[normalize-space()='{fmt}']"
            )))
            opt.click()
            break
        except Exception:
            continue

def wait_download(dirpath=DOWNLOAD_DIR, timeout=60):
    end = time.time() + timeout
    last_seen = set(p.name for p in dirpath.glob("*"))
    while time.time() < end:
        files = list(dirpath.glob("*"))
        # existe arquivo finalizado (não .crdownload)
        done = [f for f in files if f.is_file() and f.suffix != ".crdownload"]
        if [f for f in done if f.name not in last_seen]:
            return True
        time.sleep(1)
    raise TimeoutException("Download não concluiu a tempo.")

def main():
    driver = make_driver(headless=False)  # mude para True quando estabilizar
    try:
        driver.get(URL)
        close_cookies(driver)
        assert goto_pentaho_iframe(driver), "Iframe do Pentaho não encontrado."

        # Produto = Milho
        for lbl in ["Produto","Produtos"]:
            try:
                select_by_label(driver, lbl, "Milho"); break
            except Exception:
                pass

        # Periodicidade = Mensal
        for lbl in ["Periodicidade","Frequência","Periodicidade de Preços"]:
            try:
                select_by_label(driver, lbl, "Mensal"); break
            except Exception:
                pass

        # Período: mínimo → máximo
        pick_min_max_period(driver)

        # Executar pesquisa se houver
        click_search_if_any(driver)

        # Espera o painel renderizar e o botão 'Dados' existir
        wait_render(driver, timeout=90)

        # Clica no exportElement 'Dados'
        click_export_dados(driver, timeout=20)

        # Aguarda arquivo aparecer na pasta
        wait_download(DOWNLOAD_DIR, timeout=120)
        print(f"✔ Arquivo baixado em: {DOWNLOAD_DIR.resolve()}")

    finally:
        # driver.quit()
        pass

if __name__ == "__main__":
    main()
