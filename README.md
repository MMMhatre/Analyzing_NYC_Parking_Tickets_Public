# **Analyzing patterns and trends of NYC Parking tickets**


<p align="center">
    This project is part of CDAC PG DBDA-2023, March collaborated under Group-5
</p>


### **Overview:**

_This project aims to analyze New York City's parking ticket data to uncover patterns and trends in parking violations. It focuses on factors such as when, where, and why violations occur and provide insights for more effective enforcement, policy improvements, and better public understanding of parking regulations_.

-----------

### **Data Dictionary:**

| **SR.NO** | **Column Name**       | **Data Type** | **Description**                                              |
| --------- | --------------------- | ------------- | ------------------------------------------------------------ |
| 1         | Summons Number        | bigint        | A unique identifier for  each parking ticket.                |
| 2         | Plate ID              | string        | The license plate number  of the vehicle that received the parking ticket. |
| 3         | Registration State    | string        | The state in which the  vehicle is registered.               |
| 4         | Plate Type            | string        | The type of license  plate (e.g. passenger, commercial etc.) |
| 5         | Issue Date            | date          | The date when the  parking ticket was issued.                |
| 6         | Vehicle Body Type     | string        | The body type or style  of the vehicle.                      |
| 7         | Vehicle Make          | string        | The make or manufacturer  of the vehicle.                    |
| 8         | Issuing Agency        | string        | The agency that issued  the parking ticket.                  |
| 9         | Issuer Code           | integer       | A code representing the  issuer of the parking ticket.       |
| 10        | Violation Time        | timestamp     | The time of day when the  violation occurred.                |
| 11        | Violation County      | string        | The county where the violation  occurred.                    |
| 12        | Violation Description | string        | A description of the  parking violation.                     |
| 13        | Fine Amount           | decimal       | The amount of the fine  for the parking violation.           |
| 14        | Penalty Amount        | decimal       | The penalty amount  associated with the violation.           |
| 15        | Reduction Amount      | decimal       | Any reduction in the  fine amount.                           |
| 16        | Amount Due            | decimal       | The total amount due for  the parking ticket including fines and  penalties. |
| 17        | Violation Status      | string        | The status of the  violation.                                |



------------------------------

### **Major Components:**

| Component             | Description                                                  |
| --------------------- | ------------------------------------------------------------ |
| `AWS S3` , `AWS RDS`  | Raw data source.                                             |
| `AWS S3`              | Data lake.                                                   |
| `AWS Secrets Manager` | Storage and retrieval of RDS credentials in secure manner.   |
| `AWS Redshift`        | DWH  enriched data .                                         |
| `AWS Glue`            | Managing  PyScripts, orchestrating workflow with triggers.   |
| `AWS Step Functions`  | Co-ordinating the Glue workflow.                             |
| `AWS EvetBridge`      | Making event-driven trigger after successful creation of CFT. |
| `AWS SNS`             | Messaging  authenticated team-members about dynamically created Redshift's endpoint,username and database name. |
| `AWS CFT`             | Managing  entire infrastructure resources such as S3, Redshift, Secrets manager, Glue in an automated and secure manner. |
| `Tableau`             | Visual analytics and report creation to get meaningful insights |



-----------

### **Architecture**:

![nyc_arch](https://github.com/Group5-DBDA-March2023/Analyzing_NYC_Parking_Tickets/blob/VJ_auto/nyc_arch.png)

### **Step Function**:
![step_function](https://github.com/Group5-DBDA-March2023/Analyzing_NYC_Parking_Tickets/blob/VJ_auto/stepfunctions_graph.png)


---------------------

### **Implementation:**

1. **Git-actions**:  Run `deploy_cft.yml` through git actions from `main-branch`. 

2. **Execution**:  `deploy_cft.yml` will run implicitly `cft.yml`. This will be visible under the AWS CFT service of an authenticated AWS user . It will execute the entire AWS resources in an automated manner. Refer the above architecture. 

3. `grp5cft.yml` automates - 

   - Data pipelining process to get raw data  using glue scripts `RDS_ingestion.py`. and `S3_ingestion.py`
   - Ingest the source into data lake
   - Glue script for transformation using `Transform.py`
   - Redshift processes on enriched data for analytics
   - Authenticated users will receive Redshift's endpoint, username and database name.
   - Insights using `tableau`

### **Link for Tableau Public:**

[ClickMe](https://public.tableau.com/views/NYCParkingTicketsViolationAnalysis/Home?:language=en-US&:display_count=n&:origin=viz_share_link)
