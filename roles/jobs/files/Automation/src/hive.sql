USE ${db_name};

-- Query-1: ( List hospitals which received funding, break up across specialities for each company )

select Teaching_Hospital_Name,Principal_Investigator_1_Specialty,Principal_Investigator_2_Specialty,Principal_Investigator_3_Specialty,Principal_Investigator_4_Specialty,Principal_Investigator_5_Specialty,sum(Total_Amount_of_Payment_USDollars) as total_investment FROM ${rsrch_pay_table} GROUP BY Teaching_Hospital_Name,Principal_Investigator_1_Specialty,Principal_Investigator_2_Specialty,Principal_Investigator_3_Specialty,Principal_Investigator_4_Specialty,Principal_Investigator_5_Specialty;



-- Query-2: ( List companies yearly investment for hospitals )

select Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name as Company , Teaching_Hospital_Name , Program_Year , sum(Total_Amount_of_Payment_USDollars) FROM ${rsrch_pay_table} GROUP BY Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name , Teaching_Hospital_Name , Program_Year;

