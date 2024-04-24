# Proof of Concept Framework for Data Migration into Salesforce.com

## Objective

I think I've been passed over by several recruiters for Salesforce.com data migration gigs with the excuse that I can't demonstrate specific skills of loading data into Salseforce.com. My arguments that I 'was exposed' to SFDC on several previous projects, that I worked with many other CRMs and that these conversations are like "asking an experienced carpenter if he can build an orange house" seem to fall in deaf ears. I guess everybody says that. OK, so here is me show-and-telling: I know how to migrate data into Salesforce.com.

##  Source Data

I am getting a public dataset of real company addresses from the government. For free (paid by my taxes, but never mind).

Open [Mine Data Retrieval System](https://www.msha.gov/data-and-reports/mine-data-retrieval-system) from  [Mine Safety and Health Administration](https://www.msha.gov/) of the U.S. Department of Labor.

Extended Search> Advanced Search - Mines> Unselect all Filters > Run Document

Export to Excel: Mine, Current Operator, Mine Address

![](pictures/mines_download.png)

Extended Search> Advanced Search -Contractors> Unselect all Filters > Run Document. Export to Excel.

 ![](pictures/contractors_download.png)

I saved all downloaded source files into the `data` folder: [src_mine_information.xlsx](data/src_mine_information.xlsx), [src_addresses_of_record.xlsx](data/src_addresses_of_record.xlsx), [src_operator_report.xlsx](data/src_operator_report.xlsx), and [src_contractors.xlsx](data/src_contractors.xlsx). We'd work with `src_contractors.xlsx` for now. I'll save the rest for the future exercises.

## Getting Salesforce.com developer account

[Sign up for your Salesforce Developer Edition](https://developer.salesforce.com/signup). Save your user name and password.

## Infrastructure

### Environment setup 

I developed this project locally on my Windows 11 laptop. I kept the code OS agnostic. If you want to run it locally install Python (I recommend [Anaconda](https://www.anaconda.com/download/success) distribution) and [Visual Studio Code](https://code.visualstudio.com/download).

Alternatively, you can run this code on GitHub [codespaces](https://github.com/codespaces): fork this repository to your GitHub account. Click on **Code** and then **Create codespace on main**.

 

### Authentication and secret management

## Data Migration Methodology via steps to reproduce this project 

### Source Tables Snapshot

### Target Tables Snapshot

### Column Mapping

### Create or Refresh Staging Table

### Produce Pre-Load report

### Load into Target System

### Produce Post-Load Report

### Summary
