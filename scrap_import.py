import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
from datetime import datetime  
from datetime import timedelta  
import time
from numify import numify
from numpy import savetxt
import ast
import concurrent.futures
import threading
import re
from random import choice
from random import randint
links = []

col = {'Date', 'Master BOL #', 'House BOL #', 'Shipper Name', 'Shipper Address', 'Consignee Name', 'Consignee Address', 'Notify Party Name', 'Notify Party Address', 'Bill Type', 'Voyage #', 'Vessel Name', 'IMO #', 'Vessel Country', 'Place of Receipt', 'Foreign Port of Lading', 'Port of Unlading', 'Weight', 'Weight in KG', 'Quantity', 'Container', 'Commodity'}
df = pd.DataFrame(columns = col)
lock = threading.Lock()  
"""
    
    Scrap the link to each vessel page
    Return : list of link
    
"""
def scrap_vessels() :
    t0 = time.time()
    url = "https://www.importinfo.com/vessels"
    r = requests.get(url)
    soup = BeautifulSoup(r.content, "html.parser")
    nb_page = int(soup.find("ul","pagination bootpag mb-3 m-0").find_all("li")[-1].text)
    vessels = []
    for i in  tqdm(range(1,nb_page+1), position=0, leave=True)  : 
        r = requests.get(url+"?page="+str(i))
        soup = BeautifulSoup(r.content, "html.parser")    
        for i in soup.find("tbody").find_all("tr") :
            vessels.append(i.find("td").find("a")["href"])

    t1 = time.time()
    print(f"{round((t1-t0),2)} seconds to download {len(vessels)} vessels links.")
    return vessels 





"""

    Scrap the links to a search matching a vessel and each port it is related
    Input : Links to each vessels
    Return : list of link and number of manifests for each link
    
"""
def scrap_ports(vessels) :
    ports = []
    errors = []
    t0 = time.time()
    for i in tqdm(range(0,len(vessels)), position=0, leave=True) :
        try :
            r = requests.get(vessels[i])
            soup = BeautifulSoup(r.content, "html.parser")
            for j in soup.find_all("tbody")[3].find_all("tr") :
                ports.append({"Link" : j.find_all("td")[-1].find("a")["href"], "Number" : j.find_all("td")[4].text})
        except :
            errors.append(ports[i])

    t1 = time.time()
    print(f"{round((t1-t0)/60,2)} minutes to download {len(ports)} ports links.")
    
    return ports

"""

    Determine the right function to call for a given port & vessel search
    If there are more than 10k results : call scrap_manifests_links
    otherwise call : scrap_all_manifests

"""
def s(port) : 
    if port["Number"][-1] == "k" :            
        p = numify.numify(port["Number"])
        
    else :
        p = int(port["Number"].replace(",",""))
    
    
    if p >= 10000 :
        scrap_manifests_links(port["Link"])
        
        
    else :
        scrap_all_manifests(port["Link"])



"""

    Scrap all manifests links
    Input : links to all links to search combining port and vessel
    Return : List of links to all manifests
    
"""
def scrap_all_manifests(url) :
    page = "?page="
    tmp = []
    
    global links


    r = requests.get(url)
    soup = BeautifulSoup(r.content, "html.parser")
    time.sleep(randint(1,3))
    
    if soup.find_all("a", href=re.compile("page=")) != []  :
        m_page = int(soup.find_all("a", href=re.compile("page="))[-1].text)         
                    
    else :           
        m_page = -1
    
        
        
    if m_page == -1 :
        
        if len(soup.find_all("td",text = "Full Manifest")) > 1 :
            for i in soup.find_all("td",text = "Full Manifest") :
                tmp.append(i.find("a")["href"])
            
        elif  len(soup.find_all("td",text = "Full Manifest")) == 1:
            tmp.append(soup.find("td",text = "Full Manifest").find("a")["href"])
            
        
    else :
        for k in range(1,m_page+1) :
            
            r = requests.get(url.split("?")[0]+page+ str(k) +url.split("?")[1])
            soup = BeautifulSoup(r.content, "html.parser")                
            time.sleep(randint(1,3))
            if len(soup.find_all("td",text = "Full Manifest")) > 1 :
                for i in soup.find_all("td",text = "Full Manifest") :
                    tmp.append(i.find("a")["href"])
                
            elif  len(soup.find_all("td",text = "Full Manifest")) == 1:
                tmp.append(soup.find("td",text = "Full Manifest").find("a")["href"])
          
    
    lock.acquire()    
    links.append(tmp)
    
    lock.release()
      


"""
    
    Scrap information from a manifest
    Input : link to manifest
    Return : dictionnary containing the informations of the manifest
    
"""
def scrap_manifest(link) :
    r = requests.get(link)
    soup = BeautifulSoup(r.content, "html.parser")
    tmp = {}
    global df
    try :
        for i in soup.find("tbody").find_all("tr") :
            tmp.update({i.find("th").text : i.find("td").text})
            
        df = df.append(tmp, ignore_index = True)
        
        
    except :
        print("Missing link" : link)
        pass
     
    time.sleep(0.1)
    return df
    


"""

    Scrap manifests links that exceed 10k results
    Scrap data weekly from 2019-03-01 until today
    Input : URL from a search combining a port and a vessels
    Return : list of links to manifests

"""
def scrap_manifests_links(url) :
    link_tmp = []
    imo = url.split("?")[1].split("&")[0].replace("imo=","")
    po = url.split("?")[1].split("&")[1].replace("port=","")
    start_date = datetime.fromisoformat("2019-03-01")
    end_date = datetime.now() 
    delta = timedelta(weeks=1)
    ma_date = "&max_arrival_date="
    mi_date = "&min_arrival_date="
    page = "?page="

    global links
    while start_date <= end_date:
        r = requests.get(url.split("?")[0]+"?imo="+imo+mi_date+start_date.strftime('%Y-%m-%d')+ma_date+(start_date+delta).strftime('%Y-%m-%d')+"&port="+po)
        
        soup = BeautifulSoup(r.content, "html.parser")
        time.sleep(randint(3,7))
        if soup.find_all("a", href=re.compile("page=")) != []  :
            m_page = int(soup.find_all("a", href=re.compile("page="))[-1].text)         
                        
        else :           
            m_page = -1
            
    
        if m_page == -1 :   
                
                
            if len(soup.find_all("td",text = "Full Manifest")) > 1 :
                for i in soup.find_all("td",text = "Full Manifest") :
                    link_tmp.append(i.find("a")["href"])
                
            elif  len(soup.find_all("td",text = "Full Manifest")) == 1:
                link_tmp.append(soup.find("td",text = "Full Manifest").find("a")["href"])
            

        else :
            r = requests.get(url.split("?")[0]+"?imo="+imo+mi_date+start_date.strftime('%Y-%m-%d')+ma_date+(start_date+delta).strftime('%Y-%m-%d')+"&port="+po)
            soup = BeautifulSoup(r.content, "html.parser")   
            time.sleep(randint(3,7))
            for j in soup.find_all("td",text = "Full Manifest") :
                link_tmp.append(j.find("a")["href"])
            
            for i in range(2,m_page+1) :
                r = requests.get(url.split("?")[0]+page+str(i)+"&imo="+imo+mi_date+start_date.strftime('%Y-%m-%d')+ma_date+(start_date+delta).strftime('%Y-%m-%d')+"&port="+po)
                soup = BeautifulSoup(r.content, "html.parser")   
                
                time.sleep(randint(3,7))
                if len(soup.find_all("td",text = "Full Manifest")) > 1 :
                    for i in soup.find_all("td",text = "Full Manifest") :
                        link_tmp.append(i.find("a")["href"])
                    
                elif  len(soup.find_all("td",text = "Full Manifest")) == 1:
                    link_tmp.append(soup.find("td",text = "Full Manifest").find("a")["href"])
                    
                    
        

        start_date += delta
       
        
     
    links.append(link_tmp)

"""

    Load Vessel x Ports research links from file
    return list of Vessel x Ports research links

"""
def load_ports() :
    with open(r"ports.txt") as f:
        ports = f.readlines()
    
    
    for i in range(len(ports)) :
        ports[i] = ast.literal_eval(ports[i])
        
    return ports


"""

    Load manifests links from file
    return list of manifests links

"""
def load_manifests() :
    with open(r"manifests.txt") as f:
        manifests = f.readlines()
    
    for i in range(len(manifests)) :
        manifests[i] = manifests[i].replace("\n","")
        
    return manifests




        
if __name__ == "__main__":
    
    
    
    
    vessels = scrap_vessels()
    savetxt(r"E:\work\import\vessels.txt", vessels, delimiter=',', fmt = '%s')
    ports = scrap_ports(vessels)
    savetxt(r"ports.txt", ports, delimiter=',', fmt = '%s')
    
    
    
    
    ports = load_ports()
    
    
    outfile = open(r"last.txt", 'r')
    start = int(outfile.read())   
    outfile.close() 
    
    # 5000 links take 45 mn
    end = len(ports)
    
    
    
    
    
    
    
    
    for i in tqdm(range(start,end, 1000), position=0, leave=True) :
        t0 = time.time()
        
        
        with concurrent.futures.ThreadPoolExecutor(8) as executor:   
            executor.map(s, ports[i:(i+1000)])
           
        tmp =  [item for sublist in links for item in sublist] 
        f=open(r"manifests.txt",'a')
        savetxt(f, tmp, delimiter=',', fmt = '%s')
        f.close()
        links = []
        tmp = []
        t1 = time.time()
    
        outfile = open(r"last.txt", 'w')
        outfile.write(str(i+1000))      
        outfile.close() 
        
        print(f"{round((t1-t0)/60,2)} minutes to download 1000 manifests links.")
    
    
    
    
    # 5000 records take 4 minutes
    outfile = open(r"start.txt", 'r')
    start_m = 0 
    outfile.close()
    t0 = time.time()
    
    
    manifests = load_manifests()
    end_m =  len(manifests)       
    
    for i in tqdm(range(start_m, len(manifests), 50000), position=0, leave=True) :
    
        with concurrent.futures.ThreadPoolExecutor(8) as executor:
            executor.map(scrap_manifest, manifests[i:i+50000])
                
    
     
        t1 = time.time()
        outfile = open(r"start.txt", 'w')
        outfile.write(str(i))      
        outfile.close()    
        print(f"{round((t1-t0)/60,2)} minutes to download {end_m-start_m} manifests.")
        df = df[['Date', 'Master BOL #', 'House BOL #', 'Shipper Name', 'Shipper Address', 'Consignee Name', 'Consignee Address', 'Notify Party Name', 'Notify Party Address', 'Bill Type', 'Voyage #', 'Vessel Name', 'IMO #', 'Vessel Country', 'Place of Receipt', 'Foreign Port of Lading', 'Port of Unlading', 'Weight', 'Weight in KG', 'Quantity', 'Container', 'Commodity']]
        df.to_csv(r"imports.csv", mode='a', index=False, header=False)
        df = pd.DataFrame(columns = col)
        
    
    