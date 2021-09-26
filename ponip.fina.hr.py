# -*- coding: cp1250 -*-
import csv
import json
import os.path
import re
import threading
import traceback
from datetime import datetime, time
from time import sleep

import requests
from bs4 import BeautifulSoup
from xlsxwriter.workbook import Workbook

testing = False
encoding = 'utf8'
outfile = 'out-ponip.fina.hr.csv'
logfile = "log-ponip.fina.hr.csv"
logxl = 'log-ponip.fina.hr.xlsx'
errorfile = 'error-ponip.fina.hr.txt'
threadcount = 5

licitacija = 'https://licitacija.hr/upload.php'

url = 'https://ponip.fina.hr/ocevidnik-web'
headers = ["Nadležno tijelo", "Poslovni broj spisa", "Opis", "Složenost predmeta prodaje", "Utvrđena vrijednost",
           "Napomena", "Link", "Način prodaje", "ID nadmetanja", "Oznaka elektroničke javne dražbe",
           "Datum odluke o prodaji", "Datum i vrijeme početka elektroničke javne dražbe", "Time",
           "Datum i vrijeme početka nadmetanja", "Time", "Datum i vrijeme završetka nadmetanja", "Time",
           "Mogućnost produljenja nadmetanja ", "Ostali uvjeti prodaje",
           "Minimalna zakonska cijena ispod koje se predmet prodaje ne može prodati", "Početna cijena za nadmetanje",
           "Iznos dražbenog koraka", "Trenutačna cijena predmeta prodaje u nadmetanju",
           "Rok u kojem je kupac dužan položiti kupovninu", "Iznos jamčevine", "Rok za uplatu jamčevine",
           "Ostali uvjeti za jamčevinu", "Razgledavanje", "Napomena", 'uuid', "ZEMLJIŠNOKNJIŽNI ULOŽAK/PODULOŽAK",
           "KATASTARSKA ČESTICA	", "ŽUPANIJA", "GRAD/OPĆINA"]

semaphore = threading.Semaphore(threadcount)
lock = threading.Lock()
convert = False


def scrape(uuid):
    with semaphore:
        try:
            print("Scraping", uuid)
            soup = None
            for i in range(3):
                try:
                    if testing:
                        print(f'{url}/predmet_prodaje/{uuid}?src=6')
                    soup = BeautifulSoup(requests.get(f'{url}/predmet_prodaje/{uuid}?src=6').content, 'lxml')
                    break
                except:
                    if testing:
                        traceback.print_exc()
                    pass
            if soup is None:
                raise Exception
            div = soup.find('div', {'id': 'iCaptchaDownloadPismena'}).parent
            rows = []
            for row in div.find_all('div', {'class': 'row'}):
                try:
                    t = row.find_all('div')[1].text.strip().replace('\n', ' ').replace('\t', ' ').replace('  ', ' ')
                    rows.append(t)
                except:
                    pass
            data = []
            try:
                datetime.strptime(rows[11], '%d.%m.%Y').date()
                m = 0
            except:
                m = 1
            blocked = ["Link", "Trenutačna cijena", "Time", "Datum", "Pismena vezana"]
            temp = [div.find('a', {'title': 'Pismena vezana uz predmet prodaje objavljena na Javnoj objavi'})['href'],
                    datetime.strptime(rows[11 + m], '%d.%m.%Y').date(),
                    datetime.strptime(rows[12 + m].split(" ")[0], '%d.%m.%Y').date(),
                    datetime.strptime(rows[12 + m].split(" ")[1], '%H:%M:%S').time(),
                    datetime.strptime(rows[13 + m].split(" ")[0], '%d.%m.%Y').date(),
                    datetime.strptime(rows[13 + m].split(" ")[1], '%H:%M:%S').time(),
                    datetime.strptime(rows[14 + m].split(" ")[0], '%d.%m.%Y').date(),
                    datetime.strptime(rows[14 + m].split(" ")[1], '%H:%M:%S').time(),
                    div.find('p', {'id': 'trenutna-cijena-label'}).findNext('div').text.strip().replace('\n',
                                                                                                        ' ').replace(
                        '\t', ' ').replace('  ',
                                           ' ')
                    ]
            for h in headers[:-6]:
                if not any(word in h for word in blocked):
                    d = get(div, h).strip()
                    if d.endswith("kn"):
                        d = float(re.findall("\d+\.\d+", str(d).replace('.', '').replace(',', '.'))[0])
                else:
                    d = temp.pop(0)
                    try:
                        d = float(d)
                    except:
                        pass
                data.append(d)
            data.append(div.find_all(text="Napomena")[1].findNext('div').text.strip().replace('\n', ' ').
                        replace('\t', ' ').replace('  ', ' '))
            data.append(uuid)
            table = div.find('table', {'id': 'nekretnineDetaljiTablica'})
            if table is not None:
                for tr in table.find('tbody').find_all('tr'):
                    for td in tr.find_all('td')[-4:]:
                        try:
                            data.append(int(td.text))
                        except:
                            data.append(td.text)
            print(uuid, data)
            append(outfile, data)
        except:
            print("Error on", uuid)
            traceback.print_exc()
            with open(errorfile, 'a') as efile:
                efile.write(uuid + "\n")


def get(div, txt):
    return div.find(text=txt).findNext('div').text.strip().replace('\n', ' ').replace('\t', ' ').replace('  ', ' ')


def append(f, row):
    global convert
    with lock:
        with open(f, 'a', encoding=encoding, newline='') as o:
            csv.writer(o).writerow(row)
        with open(logfile, 'a', encoding=encoding, newline='') as o:
            csv.writer(o).writerow(row)
        convert = True


def csvtoxlsx():
    global convert
    while True:
        # time.sleep(10)
        if convert:
            with lock:
                print("Converting to XLSX...")
                cvrt()
            convert = False


def cvrt():
    workbook = Workbook(logxl)
    worksheet = workbook.add_worksheet()
    with open(outfile, 'rt', encoding='utf8') as f:
        reader = csv.reader(f)
        for r, row in enumerate(reader):
            for c, col in enumerate(row):
                worksheet.write(r, c, col)
    workbook.close()


def main():
    os.system('color 0a')
    logo()
    try:
        print('Press Ctrl+C to skip waiting...')
        wait_start('17:00')
    except KeyboardInterrupt:
        print('Waiting skipped...')
    # cvrt()
    # print(requests.post(licitacija, files={'file': open(logxl, 'rb')}))
    # input("Press any key...")
    print("Please wait, loading data...")
    if not os.path.isfile(outfile) or testing:
        with open(outfile, 'w', encoding=encoding, newline='') as o:
            csv.writer(o).writerow(headers)
    with open(logfile, 'w', encoding=encoding, newline='') as o:
        csv.writer(o).writerow(headers)
    if testing:
        scrape('a8974bfd-0951-46a7-af11-95e8d8241d15')
        return
    with open(outfile, 'r', encoding=encoding) as o:
        lines = o.read()
    js = json.loads(requests.get(f'{url}/pregled/najava').text)
    srtd = sorted(js, key=lambda x: datetime.strptime(x['datPocFmt'], '%d.%m.%Y'))
    srtd.reverse()
    # threading.Thread(target=csvtoxlsx).start()
    # if os.path.isfile(errorfile):
    #     with open(errorfile, 'r') as efile:
    #         elines = efile.read().splitlines()
    #     if len(elines) > 0:
    #         print("Working on error file")
    #     for eline in elines:
    #         threading.Thread(target=scrape, args=(eline,)).start()
    #     print("Work on error file finished! now working on fresh data!")
    threads = []
    for d in srtd:
        if d["uuid"] not in lines:
            t = threading.Thread(target=scrape, args=(d["uuid"],))
            threads.append(t)
            t.start()
        else:
            print("Already scraped", d['uuid'])
    for thread in threads:
        thread.join()
    with lock:
        print("Converting to XLSX...")
        cvrt()
    print("Uploading...")
    print(requests.post(licitacija, files={'file': open(logxl, 'rb')}))
    print("Done!")


def logo():
    print(f"""
                    _              __ _                   _          
                   (_)            / _(_)                 | |         
  _ __   ___  _ __  _ _ __       | |_ _ _ __   __ _      | |__  _ __ 
 | '_ \ / _ \| '_ \| | '_ \      |  _| | '_ \ / _` |     | '_ \| '__|
 | |_) | (_) | | | | | |_) |  _  | | | | | | | (_| |  _  | | | | |   
 | .__/ \___/|_| |_|_| .__/  (_) |_| |_|_| |_|\__,_| (_) |_| |_|_|   
 | |                 | |                                             
 |_|                 |_|                                             
========================================================================
         ponip.fina.hr scraper by: fiverr.com/muhammadhassan7
========================================================================
[+] Multithreaded
[+] Resumeble
[+] Without browser
[+] Upload new logs to licitacija.hr
Threadcount: {threadcount}
""")

def wait_start(runTime):
    startTime = time(*(map(int, runTime.split(':'))))
    while startTime > datetime.today().time():
        sleep(1)
        print(f"Waiting for {runTime}")
if __name__ == "__main__":
    main()
