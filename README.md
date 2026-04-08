# Participants and Services Data Quality Checker

A Python tool for auditing the data quality of consolidated Excel databases focused on the **`participantes`** and **`servicios`** worksheets.

The tool is designed for Excel-based data collection processes where multiple users fill mirrored spreadsheet files. It evaluates data completeness, detects logical inconsistencies, identifies duplicates, checks relational integrity, and generates an actionable Excel report with the records that need correction.

## Features

- Evaluates **required field completeness** in participant and service records
- Detects **logical validation errors**
- Flags **duplicate participants** and **duplicate service IDs**
- Identifies **orphan records**, including:
  - services linked to missing participants
  - participants with no associated services
- Filters the analysis by **month** or **range of months**
- Generates an Excel report with:
  - row-level summary
  - cell-level summary
  - actionable correction sheet

## Expected input

The input file must contain at least these worksheets:

- `participantes`
- `servicios`

English equivalents are also supported:

- `participants`
- `services`

The tool normalizes column names and supports alternative column labels for required fields.

## What the tool validates

### 1. Completeness checks

#### Participants
Required fields include:

- Participant type
- Full name
- Document type
- Document number
- Date of birth
- Sex
- Gender
- Department of residence
- Municipality of residence

Additional required fields for PARD cases:

- Defender
- PARD opening date

#### Services
Required fields include:

- Program
- Service group
- Service
- Sub-service
- Family of origin
- Foster family / care home
- Date of entry into Aldeas
- Date of entry into service
- Reason for entering the service

Some completeness rules are conditional depending on the service type.

### 2. Logical validations

The tool detects issues such as:

- future or invalid birth dates
- mismatch between declared age and calculated age
- age group inconsistent with age
- invalid document number format for the declared document type
- duplicated participant records
- invalid PARD opening dates
- future service dates
- Aldeas entry date later than service entry date
- service exit date earlier than service entry date
- inconsistent family fields depending on service type
- duplicated service IDs
- orphan service records
- participants without services
- service entry date earlier than date of birth
- unusual participant type for a service group

## Output

The tool generates an Excel file named like this:

CALIDAD_PS_<period>_<input_filename>_<timestamp>.xlsx

It includes three worksheets:

resumen_por_fila

Row-level quality summary by program and department.

resumen_por_celda

Cell-level quality summary, including expected cells, missing values, and validity percentages.

casos_a_corregir

Operational correction guide listing the records with missing data, logical errors, and warnings.

Color coding:

Red: missing required data
Orange: logical errors without missing required data

## Installation

Recommended Python version: 3.10+

## Usage

Run the script
Select the month or date range to evaluate
Select the Excel input file
Wait for the processing to finish
Review the generated Excel report

## Notes

The analysis period is based on service entry date
The tool is intended for Excel files with a specific operational structure
It does not modify the source data
It reports issues for correction in the output file

## Author

Rodrigo Vallejo Domínguez
Monitoring and Evaluation
Aldeas Infantiles SOS Colombia
