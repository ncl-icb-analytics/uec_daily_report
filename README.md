# UEC Daily Report

<!-- ABOUT THE PROJECT -->
## About The Project

This git repository contains an all in one python script to process daily updates for the UEC Daily Dashboard. The source files for each dataset used in the dashboard need to be prepared manually but this code will process them and upload them to the sandpit such that the excel dashboard can pull new data from a refresh. This is designed to be used with v2.1 of the UEC Daily Dashboard Template excel.

The intention is to replace this report with a daily infographic but this code is designed to be used until said replacement is developed.

In general, it is recommended to refer to the SOP for the dashboard for the most complete guide on maintaining the dashboard:
`N:\Performance&Transformation\Performance\NELCSUNCLMTFS\_DATA\UEC Daily Report\SOP Archive`

## First Time Installations

Refer to the SOP stored in the below directory for instructions on what is required to run this code:
`N:\Performance&Transformation\Performance\NELCSUNCLMTFS\_DATA\UEC Daily Report\SOP Archive`

## Usage

In order to run, daily data files are required for most of the datasets involved. These files are saved at:
`N:\Performance&Transformation\Performance\NELCSUNCLMTFS\_DATA\UEC Daily Report\data`
with details on how to store the files in the SOP for the dashboard.

When all daily preperations are complete, open the uec_daily_dashboard_processing.py file in the src folder and execute the code. This should bring up a terminal with details on the current execution.

## Licence
This repository is dual licensed under the [Open Government v3]([https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/) & MIT. All code can outputs are subject to Crown Copyright.

## Contact
Jake Kealey - jake.kealey@nhs.net

Project Link: https://github.com/ncl-icb-analytics/uec_daily_report
