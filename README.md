# Azure ETL Pipeline: End-to-End Data Ingestion and Transformation with Data Factory & Databricks

## Table of Contents
1. [Introduction / Overview](#introduction--overview)
2. [Technologies Used](#technologies-used)
3. [Architecture](#architecture)
4. [Implementation](#implementation)
   - [Part 1: Azure Setup](#part-1-azure-setup)
   - [Part 2: Azure Data Factory](#part-2-azure-data-factory)
   - [Part 3: Azure Databricks](#part-3-azure-databricks)
5. [Conclusion](#conclusion)

---

## Introduction / Overview

This project demonstrates how to build a simple, end-to-end ETL (Extract, Transform, Load) pipeline on Microsoft Azure using two of its core data engineering services: **Azure Data Factory** and **Azure Databricks**. The goal is to take raw data sitting in a storage container, move it through a data pipeline, and make it available for further processing and transformation using Databricks notebooks — all while keeping everything organized under a single, well-structured resource group.

The project is intentionally kept beginner-friendly and is meant to serve as a hands-on walkthrough for anyone looking to understand how the pieces of a cloud-based ETL workflow fit together: storage, orchestration, and compute. By the end of this project, you'll have a working pipeline that copies data from a source location to a destination location in a data lake, along with a Databricks workspace ready to query and transform that data.

### Learning Resources:
[1. Basics of Azure ADF and databricks.](https://youtu.be/bSx3DbJNQk4?si=BfgRozbBLVW58mO4)  
[2. Connecting storage account to databricks.](https://youtu.be/VkjqViooMtQ?si=yODN8co9dlj7Ex4F)  
[3. Similar project](https://github.com/Sonu7804/azure-etl-sales-pipeline)

## Technologies Used

- **Microsoft Azure** – Cloud platform hosting all project resources
- **Azure Resource Group** – Logical container to organize and manage related resources
- **Azure Storage Account (Data Lake Gen2)** – Storage layer used as both the landing zone and the destination for data, with hierarchical namespace enabled
- **Azure Data Factory (ADF)** – Cloud-based ETL/orchestration service used to build and run the data movement pipeline
- **Azure Databricks** – Apache Spark-based analytics platform used for data transformation and processing
- **Azure Access Connector for Databricks** – Enables secure, identity-based access between Databricks and the storage account
- **CSV** – File format used for the sample source data

## Architecture

At a high level, the architecture follows a straightforward flow:

```
Source Container (Data Lake)
        |
        v
Azure Data Factory Pipeline (Copy Activity)
        |
        v
Destination Container (Data Lake)
        |
        v
Azure Databricks (Compute + Notebook)
        |
        v
Transformed / Processed Data
```

Everything lives inside a single **resource group**, which acts as the umbrella for the storage account, the Data Factory instance, and the Databricks workspace. The storage account is configured with a hierarchical namespace, turning it into a proper Data Lake Gen2 account rather than plain blob storage — this is what allows Databricks and ADF to interact with it more efficiently and treat it like a file system rather than flat object storage.

Data Factory handles the "E" and "L" of ETL: it pulls data from the source container and copies it into the destination container using a simple copy activity pipeline. Databricks then picks up from there, connecting to the storage account through an access connector (rather than storing credentials directly), and provides the compute environment where the actual data transformation happens inside notebooks.

## Implementation

### Part 1: Azure Setup

**Step 1: Create an Azure account**
Start by signing up for a free-tier Azure account. This gives you access to a limited amount of free credits and services, which is more than enough to follow along with this project without incurring any cost.

**Step 2: Create a resource group**
A resource group is essentially a folder for your cloud resources. Create one for this project — it will act as the base/container for every service we spin up going forward (storage account, Data Factory, Databricks, etc.), making it much easier to manage, monitor, and eventually clean up everything in one place.
<img width="1647" height="770" alt="Resource Group img" src="https://github.com/user-attachments/assets/5801ca6e-7d72-40c1-acbf-8256dc444f6f" />
<img width="1706" height="756" alt="Resource group and its services" src="https://github.com/user-attachments/assets/a3e3322c-50fb-4a36-8af0-1fc5d58b1e62" />  



**Step 3: Create a storage account**
Create a new storage account, and while setting it up, make sure to **enable the hierarchical namespace** option. This is an important step — enabling it converts the storage account from standard blob storage into an Azure Data Lake Storage Gen2 account, which supports directory-like structures and integrates more seamlessly with services like Data Factory and Databricks.
<img width="1218" height="504" alt="Storage service" src="https://github.com/user-attachments/assets/c1041efc-c531-4951-af13-74371c589880" />
<img width="824" height="826" alt="Storage create 1" src="https://github.com/user-attachments/assets/92417bcc-37db-44d5-8b22-19a61d589ebb" />
<img width="887" height="828" alt="storage create 2" src="https://github.com/user-attachments/assets/dd191779-a91e-4de3-a691-72b01182509b" />


**Step 4: Set up containers**
Navigate to the storage account and open the "Containers" tab. Create two containers: one to act as the **source** and one to act as the **destination**. Once created, upload your sample data into the source container — this is the data our pipeline will eventually pick up and move.
<img width="1917" height="767" alt="Storage account" src="https://github.com/user-attachments/assets/cc30c5cc-9cce-408a-8eed-86ab9bb53861" />

---

### Part 2: Azure Data Factory

**Step 5: Create an Azure Data Factory resource**
Provision a new Azure Data Factory (ADF) instance inside the same resource group. This service will be responsible for orchestrating the movement of data between containers.

**Step 6: Launch ADF Studio**
Once the resource is created, open it and launch the **ADF Studio** — this is the visual workspace where pipelines, datasets, and linked services are built and managed.

**Step 7: Create a linked service**
Go to the **Manage** tab in ADF Studio, select **Linked Services**, and create a new one. This linked service establishes the connection between ADF and your storage account, so the pipeline knows where to read from and write to.
<img width="1918" height="811" alt="ADF Linked service" src="https://github.com/user-attachments/assets/fc9b4bec-1fd4-42ec-85ae-2a1f640146e6" />  


**Step 8: Build the pipeline**
Switch to the **Author** tab and create a new pipeline. From the activities panel, search for the **Copy Data** activity and drag it onto the pipeline canvas — this activity is what will actually move the data from source to destination.
<img width="1640" height="773" alt="Pipeline" src="https://github.com/user-attachments/assets/29476f9e-44ed-472c-8c3c-9bb313bc6a04" />


**Step 9: Configure the copy activity**
This is where the source and sink (destination) get defined:
- For the **source**, create a new dataset pointing to Azure Data Lake Storage, set the file format to CSV, and select the source container along with the appropriate linked service.
- For the **sink**, do the same thing but pointed at the destination container.

> **Note:** Since the source and destination are both located in the same storage account in this project, only a single linked service is needed. If your source and destination lived in different storage accounts (or different services altogether), you'd need to create a separate linked service for each.
<img width="1919" height="815" alt="copy activity" src="https://github.com/user-attachments/assets/0ae582fa-70a6-4b91-9a13-66d354531691" />
<img width="790" height="473" alt="Properties" src="https://github.com/user-attachments/assets/d0f122fd-b75e-4111-9e74-190f3ca6170e" />


**Step 10: Debug the pipeline**
Before publishing anything, run the pipeline in **Debug** mode. This lets you test the copy activity and catch any configuration errors without needing to trigger a full, scheduled run.

**Step 11: Add triggers and validate**
Once the pipeline runs successfully, set up a trigger to define when/how often the pipeline should execute. Finally, click **Validate All** to check the pipeline for issues and save your changes.

---

### Part 3: Azure Databricks

**Step 12: Create an Azure Databricks resource**
Provision a new Databricks resource. When configuring it, select your existing project resource group for the main resource group, but for the **managed resource group**, give it a new, distinct name. Azure Databricks will automatically create and manage this second resource group behind the scenes.

> **Note:** Once created, avoid modifying anything inside the managed resource group unless you specifically know what you're doing — Databricks relies on it internally, and changes there can break your workspace.
<img width="808" height="863" alt="Create databricks service." src="https://github.com/user-attachments/assets/381bb97b-395f-42bc-af3d-c7cf0d528dcf" />  

**Step 13: Create an Access Connector for Azure Databricks**
Create this connector resource and copy its resource ID once it's provisioned — you'll need this to grant Databricks secure access to your storage account.
<img width="1020" height="811" alt="Access connector for databricks" src="https://github.com/user-attachments/assets/83224e2d-d31b-4467-a3c0-3be94284dc84" />  
<img width="1857" height="497" alt="Access connector ID " src="https://github.com/user-attachments/assets/fe778fa0-78b8-48c5-a6f9-325554d1059a" />  

**Step 14: Assign access on the storage account**
Head back to your storage account, open the **Access Control (IAM)** tab, and assign an appropriate role to the Databricks access connector. This allows Databricks to securely read/write data in the storage account without relying on hardcoded credentials.
<img width="1619" height="771" alt="Storage IAM" src="https://github.com/user-attachments/assets/288736b3-2bcc-4487-b647-70854f1bc55f" />
<img width="1816" height="721" alt="Storage IAM" src="https://github.com/user-attachments/assets/91d52ca4-64b6-428c-88d3-3343e5641c11" />
<img width="1858" height="812" alt="Storage IAM" src="https://github.com/user-attachments/assets/885c5acd-0c28-48b2-9a07-fa50db4bff09" />  


**Step 15: Launch the Databricks workspace**
Go into the Databricks resource and launch the workspace from the Azure portal.

**Step 16: Create a compute cluster**
Inside the workspace, go to the **Compute** tab and spin up a new cluster sized according to your project's needs.
<img width="1885" height="862" alt="Compute" src="https://github.com/user-attachments/assets/0419b105-b00c-4fe6-9c71-c2e3f79d89a8" />  


**Step 17: Set up catalog credentials and external locations**
Navigate to the **Catalog** tab and create the necessary **credentials** and **external locations**. This links Unity Catalog to your storage account so Databricks can reference data stored there directly.
<img width="798" height="781" alt="Credential" src="https://github.com/user-attachments/assets/95dd846f-db5f-4ff2-96e3-2b23f064e563" />
<img width="975" height="812" alt="External location" src="https://github.com/user-attachments/assets/011f2ec7-d630-4153-b655-306054bde564" />  


**Step 18: Create a workspace and notebook**
Set up a workspace folder and create a new notebook inside it — this is where you'll write the code to read, transform, and analyze the data that was moved into the destination container.
```
file_path = "abfss://Container_name@Storage_account.dfs.core.windows.net/Path/data.csv"
file_content = spark.read.format("csv").option("header", "true").load(file_path)
```
<img width="1400" height="138" alt="Bronze" src="https://github.com/user-attachments/assets/e022888a-b3f0-47ac-8da7-67295df66462" />
<img width="1400" height="654" alt="Silver" src="https://github.com/user-attachments/assets/7d9b875d-6473-4358-bc0a-2812f913dcd3" />
<img width="1400" height="323" alt="Gold" src="https://github.com/user-attachments/assets/12e0c056-ce83-4e37-9ea0-403c4eb86276" />   


**Step 19: Terminate the compute**
Once your work is done, remember to terminate the cluster. Databricks compute is billed while running, so shutting it down when you're finished helps avoid unnecessary charges.

## Conclusion

This project walks through building a functional, cloud-native ETL pipeline entirely within the Azure ecosystem — starting from raw infrastructure setup, through orchestrated data movement with Data Factory, and finally into a Spark-powered compute environment with Databricks for transformation and analysis.

Beyond just moving files from one container to another, this workflow highlights some genuinely important cloud engineering practices: organizing resources cleanly under a resource group, using hierarchical namespace-enabled storage to unlock data lake capabilities, connecting services securely through linked services and access connectors instead of hardcoded credentials, and being mindful of compute costs by shutting down clusters when they're not in use.

This setup can easily be extended further — additional pipeline activities, more complex transformations in Databricks notebooks, scheduled/automated triggers, or even integration with other Azure services like Synapse Analytics or Power BI for downstream reporting and visualization.
