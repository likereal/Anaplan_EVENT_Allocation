from flask import Flask
import requests
import json
import pandas
from io import StringIO
import numpy as np
from collections import Counter
import sys
import csv
import os

app = Flask(__name__)

@app.route('/')
def mainWork():
    username = "prabhu@relanto.ai"
    password = "Anaplan@April2023"
    
    auth_url = 'https://auth.anaplan.com/token/authenticate'
    auth_json = requests.post(
        url=auth_url,
        auth=(username, password)
    ).json()
    if auth_json['status'] == 'SUCCESS':
        authToken = 'AnaplanAuthToken ' + auth_json['tokenInfo']['tokenValue']
        print("AnaplanAuthToken : " + auth_json['status'])
        
        '''Token Validation'''
        auth_url = 'https://auth.anaplan.com/token/validate'
        auth_json2 = requests.get(
            url=auth_url,
            headers={
                'Authorization': authToken
            }
        ).json()
        print("Token Validation : " + auth_json2['status'])
        if auth_json2['status'] == 'SUCCESS':
            expiresAt = auth_json2['tokenInfo']['expiresAt']
            print("Auth Token Validation : " + auth_json2['status'])
            
            ExportProcess = "Export from Anaplan to Python"
        
            
            #Getting Process from Anaplan
            auth_url = 'https://api.anaplan.com/2/0/workspaces/8a868cdc7bd6c9ae017be5b938c83112/models/94E1B92C9FD34262BE156ED588F89FDF/processes'
            auth_json3= requests.get(
                url=auth_url,
                headers={
                    'Authorization': authToken
                }
            ).json()
            print("Getting Process from Anaplan : " + auth_json3['status']['message'])
            if auth_json3['status']['message'] == 'Success':
                for process in auth_json3['processes']:
                    if ExportProcess == process['name']:
                        processID = process['id']
                        print("Anaplan Process ID " + processID)
                        #Starting the Process
                        auth_url = f"https://api.anaplan.com/2/0/workspaces/8a868cdc7bd6c9ae017be5b938c83112/models/94E1B92C9FD34262BE156ED588F89FDF/processes/{processID}/tasks"
                        auth_json4 = requests.post(
                            url=auth_url,
                            headers={
                                'Authorization': authToken,
                                'Content-type': 'application/json'
                            },
                            data = json.dumps({'localeName': 'en_US'})
                        ).json()
                        print("Anaplan Process Definition : "+auth_json4['status']['message'])
                        if auth_json4['status']['message'] == 'Success':
                            taskID = auth_json4['task']['taskId']
                            print("Anaplan Process Task ID "+taskID)
                            #Checking the Status of the Process
                            Flag = True
                            while Flag:
                                auth_url = f"https://api.anaplan.com/2/0/workspaces/8a868cdc7bd6c9ae017be5b938c83112/models/94E1B92C9FD34262BE156ED588F89FDF/processes/{processID}/tasks/{taskID}"
                                auth_json5 = requests.get(
                                    url=auth_url,
                                    headers={
                                        'Authorization': authToken,
                                        'Content-type': 'application/json'
                                    }
                                ).json()
                                if auth_json5['task']['currentStep'] == "Failed.":
                                    print("Anaplan Process Failed")
                                    Flag = False
                                elif auth_json5['task']['currentStep'] == "Complete.":
                                    print("Anaplan Process Completed")
                                    Flag = False
            
            #Get files from anaplan
            url = f"https://api.anaplan.com/2/0/workspaces/8a868cdc7bd6c9ae017be5b938c83112/models/94E1B92C9FD34262BE156ED588F89FDF/files/"
            getFileData = requests.get(
                url = url,
                headers = {
                    'Authorization': authToken
                }
            )
            getFileData_json = getFileData.json()
            print("Get Files from Anaplan : "+ getFileData_json['status']['message'])

            if getFileData_json['status']['message'] == 'Success':
                file_info = getFileData_json['files'];
                
                for file in file_info:
                    if file['name'] == "Current Page - INP002 BOM Usage.csv":
                        fileID = file['id']
                        url = f"https://api.anaplan.com/2/0/workspaces/8a868cdc7bd6c9ae017be5b938c83112/models/94E1B92C9FD34262BE156ED588F89FDF/files/{fileID}/chunks/"
                        getChunk = requests.get(
                            url,
                            headers = {
                                'Authorization': authToken,
                                "Content-Type": "application/json"
                            }
                        )
                        getChunk = getChunk.json()
                        if getChunk['status']['message'] == "Success":
                            print(f"Getting the chunk count of {file['id']} COMPLETED")
                            for chunk in getChunk['chunks']:
                                url = f"https://api.anaplan.com/2/0/workspaces/8a868cdc7bd6c9ae017be5b938c83112/models/94E1B92C9FD34262BE156ED588F89FDF/files/{fileID}/chunks/{chunk['id']}"
                                getChunk = requests.get(
                                    url,
                                    headers = {
                                        'Authorization': authToken,
                                        "Content-Type": "application/json"
                                    }
                                )
                                bom = pandas.read_csv(StringIO(getChunk.text), sep=",")
                    if file['name'] == "DAT01 Open Inventory - New list - Inventory In hand.csv":
                        fileID = file['id']
                        url = f"https://api.anaplan.com/2/0/workspaces/8a868cdc7bd6c9ae017be5b938c83112/models/94E1B92C9FD34262BE156ED588F89FDF/files/{fileID}/chunks/"
                        getChunk = requests.get(
                            url,
                            headers = {
                                'Authorization': authToken,
                                "Content-Type": "application/json"
                            }
                        )
                        getChunk = getChunk.json()
                        if getChunk['status']['message'] == "Success":
                            print(f"Getting the chunk count of {file['id']} COMPLETED")
                            for chunk in getChunk['chunks']:
                                url = f"https://api.anaplan.com/2/0/workspaces/8a868cdc7bd6c9ae017be5b938c83112/models/94E1B92C9FD34262BE156ED588F89FDF/files/{fileID}/chunks/{chunk['id']}"
                                getChunk = requests.get(
                                    url,
                                    headers = {
                                        'Authorization': authToken,
                                        "Content-Type": "application/json"
                                    }
                                )
                            inventory = pandas.read_csv(StringIO(getChunk.text), sep=",")
                    if file['name'] == "Current Page - INP001 Demand.csv":
                        fileID = file['id']
                        url = f"https://api.anaplan.com/2/0/workspaces/8a868cdc7bd6c9ae017be5b938c83112/models/94E1B92C9FD34262BE156ED588F89FDF/files/{fileID}/chunks/"
                        getChunk = requests.get(
                            url,
                            headers = {
                                'Authorization': authToken,
                                "Content-Type": "application/json"
                            }
                        )
                        getChunk = getChunk.json()
                        if getChunk['status']['message'] == "Success":
                            print(f"Getting the chunk count of {file['id']} COMPLETED")
                            for chunk in getChunk['chunks']:
                                url = f"https://api.anaplan.com/2/0/workspaces/8a868cdc7bd6c9ae017be5b938c83112/models/94E1B92C9FD34262BE156ED588F89FDF/files/{fileID}/chunks/{chunk['id']}"
                                getChunk = requests.get(
                                    url,
                                    headers = {
                                        'Authorization': authToken,
                                        "Content-Type": "application/json"
                                    }
                                )
                                demand = pandas.read_csv(StringIO(getChunk.text), sep=",")
                                print(demand)
                                
                #demarr=demand['Demand'].to_numpy()
                quantity=bom['Quantity'].to_numpy()
                time=demand['Time'].to_numpy()
                country=demand['Country'].to_numpy()
                #comp_products=bom['Components'].to_numpy()
                products=demand['Plant_SKU'].unique()
                inv_time=(inventory['Time'])
                #inv_time.replace(to_replace="-",value=" ")
                #print(inv_time)
                #time_format=['Jan-23','Feb-23','Mar-23','Apr-23','May-23','Jun-23','Jul-23','Aug-23','Sep-23','Oct-23','Nov-23','Dec-23']
                countries=demand['Country'].unique()



                #this is an alternative to the above code 
                results={}

                #directory = "C:\Prabhu\Relanto\ANAPLAN_EVENT_DEMO - Allocation"
                #filename = os.path.join(directory, 'output.csv')
                #filename = "output.csv"
                #with open(filename, "a", newline="") as file:
                #  writer = csv.writer(file)
                #  writer.writerow(['Plant_SKU','Time','Country','Demand','Result'])
                
                output=pandas.DataFrame(columns=['Plant_SKU','Time','Country','Demand','Result'])
                output_Plant_SKU=[]
                output_Time=[]
                output_Country=[]
                output_Demand=[]
                output_Result=[]
                balance={}
                for mon in time:

                  inv_products=inventory[inventory['Time']==(mon)]
                  prod_i=dict(zip(inv_products['Plant_Component'],inv_products['Inventory In hand']))
                  prod_inv=Counter(prod_i)+Counter(balance)
                  # print(inventory['Time'])
                  
                  for prod in products:
                    
                    for country in countries:     
                        demand_for_mon=(demand[(demand['Time'] == mon)&(demand['Plant_SKU']==prod)&(demand['Country']==country)])

                        
                        compp=bom[bom['SKU']==prod]
                        
                        comp_name = compp[compp['Components'].str.startswith(country)]['Components'].to_numpy()
                        
                        required_comp=dict(zip(comp_name,quantity))
                        
                        minf=sys.maxsize
                        for i in (comp_name):
                              if (required_comp[i]==0):
                                continue
                              minf=min((prod_inv[i]//required_comp[i]),minf)
                       
                        if  minf > (demand_for_mon['Demand'].values[0]):
                             minf=(demand_for_mon['Demand'].values[0])

                        for i in (comp_name):
                          prod_inv[i]-=(minf*required_comp[i])
                        
                        

                        if ("{} Demand is {} for {} and country {} Result is".format(prod,demand_for_mon['Demand'].values[0], mon,country)) not in results:
                          results["{} Demand is {} for {} and country {} Result is".format(prod,demand_for_mon['Demand'].values[0], mon,country)]=minf
                          #here add all 5 columns to the csv
                          output_Plant_SKU.append(prod+"_"+country)
                          output_Time.append(mon)
                          output_Country.append(country)
                          output_Demand.append(demand_for_mon['Demand'].values[0])
                          output_Result.append(minf)
                          #with open(filename, "a", newline="") as file:
                          #      writer = csv.writer(file)
                          #      writer.writerow([prod+"_"+country,mon, country, demand_for_mon['Demand'].values[0], minf])
                        balance=prod_inv.copy() 
                output['Plant_SKU']=output_Plant_SKU
                output['Time']=output_Time
                output['Country']=output_Country
                output['Result']=output_Result
                output['Demand']=output_Demand
                                
                              
                for file in file_info:
                    if file['name'] == "output.csv":
                        fileID = file['id']
                        file['chunkCount'] = -1
                        fileData = file
                        url = f'https://api.anaplan.com/2/0/workspaces/8a868cdc7bd6c9ae017be5b938c83112/models/94E1B92C9FD34262BE156ED588F89FDF/files/{fileID}'
                        getFileData1 = requests.post(
                            url = url,
                            headers = {
                                'Authorization': authToken,
                                'Content-Type': 'application/json'
                            },
                            json = fileData
                        )
                        getFileData1 = getFileData1.json()

                        if getFileData1['status']['message'] == 'Success':
                            print(f"Setting chunk count to -1 for {file['name']} COMPLETED")
                        
                        uploadfile = output.to_csv()
                        #print(csv)
                        #test.to_csv("C:\Prabhu\Relanto\ANAPLAN_EVENT_DEMO\CAL01 Sales Forecast (4).csv")
                        tempFileName = file['name']
                        fileID = file['id']
                        #with open('output.csv', 'rb') as uploadFile:
                        #    f = uploadFile.read()
                        url = f'https://api.anaplan.com/2/0/workspaces/8a868cdc7bd6c9ae017be5b938c83112/models/94E1B92C9FD34262BE156ED588F89FDF/files/{fileID}/chunks/0'
                        requests.put(
                            url,
                            headers = {
                                'Authorization': authToken,
                                'Content-Type': 'application/octet-stream'
                            },
                            data = uploadfile
                        )
                        print("Allocated Data Uploaded to Anaplan")
                        
                        url = f'https://api.anaplan.com/2/0/workspaces/8a868cdc7bd6c9ae017be5b938c83112/models/94E1B92C9FD34262BE156ED588F89FDF/files/{fileID}/complete'
                        fileCompleteResponse = requests.post(
                        url,
                        headers = {
                            'Authorization': authToken,
                            'Content-Type': 'application/json'
                        },
                        json = file
                        )
                        fileCompleteResponse = fileCompleteResponse.json()

                        if fileCompleteResponse['status']['message'] == "Success":
                            print(f"{tempFileName} started MARKED as complete")
                            
                            #Getting Process from Anaplan
                            auth_url = 'https://api.anaplan.com/2/0/workspaces/8a868cdc7bd6c9ae017be5b938c83112/models/94E1B92C9FD34262BE156ED588F89FDF/processes'
                            auth_json3= requests.get(
                                url=auth_url,
                                headers={
                                    'Authorization': authToken
                                }
                            ).json()
                            print("Gathering Import process from Anaplan : " + auth_json3['status']['message'])
                            if auth_json3['status']['message'] == 'Success':
                                for process in auth_json3['processes']:
                                    if "Import allocation to anaplan" == process['name']:
                                        processID = process['id']
                                        print(processID)
                                        #Starting the Process
                                        auth_url = f"https://api.anaplan.com/2/0/workspaces/8a868cdc7bd6c9ae017be5b938c83112/models/94E1B92C9FD34262BE156ED588F89FDF/processes/{processID}/tasks"
                                        auth_json4 = requests.post(
                                            url=auth_url,
                                            headers={
                                                'Authorization': authToken,
                                                'Content-type': 'application/json'
                                            },
                                            data = json.dumps({'localeName': 'en_US'})
                                        ).json()
                                        print("Generating the taskID " + auth_json4['status']['message'])
                                        if auth_json4['status']['message'] == 'Success':
                                            taskID = auth_json4['task']['taskId']
                                            print(taskID)
                                            #Checking the Status of the Process
                                            Flag = True
                                            while Flag:
                                                auth_url = f"https://api.anaplan.com/2/0/workspaces/8a868cdc7bd6c9ae017be5b938c83112/models/94E1B92C9FD34262BE156ED588F89FDF/processes/{processID}/tasks/{taskID}"
                                                auth_json5 = requests.get(
                                                    url=auth_url,
                                                    headers={
                                                        'Authorization': authToken,
                                                        'Content-type': 'application/json'
                                                    }
                                                ).json()
                                                if auth_json5['task']['currentStep'] == "Failed.":
                                                    print("Failed")
                                                    Flag = False;
                                                elif auth_json5['task']['currentStep'] != "Complete.":
                                                    print("Anaplan Import Process execution "+auth_json['task']['currentStep'])
                                                elif auth_json5['task']['currentStep'] == "Complete.":
                                                    print("Anaplan Import Process execution : Completed")
                                                    Flag = False
                
    return "Integration Ran Successfull"
                                    


if __name__ == '__main__':
    app.run()
