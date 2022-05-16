# import-info_scrap
Scrap all manifests on https://www.importinfo.com/


To run the script :   python3 scrap_import.py


The script has 6 main functions :
	- scrap_vessels() : scrap the link to each vessels  --> return a list of links
	- scrap_ports(vessels) : take the list of vessels link previously scraped and scrap the links to a joint search from ports and vessels  --> return a list of links
	- scrap_all_manifests(port) : take the list of search link previously scraped and scrap the link to each manifest --> return a list of links 
		- scrap_manifests_links(url) : subfunction of scrap_all_manifests. For special case that have more than 10k results. 
										divide the search weekly to ensure that there are less than 10k results
	- scrap_manifest(link) : scrap the information from the manifest link and store the information in a dataframe		
	- load_ports() : load the links of ports
	- load_manifests() : load the links of manifests 

										
	
	
	
	
	 
	 
