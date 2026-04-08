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

```text
CALIDAD_PS_<period>_<input_filename>_<timestamp>.xlsx
