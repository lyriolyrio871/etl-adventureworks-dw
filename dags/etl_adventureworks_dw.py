from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os
from sqlalchemy import create_engine


INPUT_PATH = "/opt/airflow/input"   
OUTPUT_PATH = "/opt/airflow/output" 


engine = create_engine("postgresql://dw:dw@postgres_dw:5432/dw")


def load_csv(name):
    file = f"{INPUT_PATH}/{name}.csv"
    print(f"Lendo arquivo: {file}")
    return pd.read_csv(file, sep=";", quotechar='"', on_bad_lines="skip")

def save_csv(df, name):
    file = f"{OUTPUT_PATH}/{name}.csv"
    print(f"Salvando arquivo: {file}")
    df.to_csv(file, index=False)

def drop_common_columns(df):
    cols_to_drop = ["rowguid", "ModifiedDate"]
    return df.drop(columns=[c for c in cols_to_drop if c in df.columns], errors="ignore")


def extract_data():
    df_customer = drop_common_columns(load_csv("Customer"))
    df_person = drop_common_columns(load_csv("Person"))
    df_sales_header = drop_common_columns(load_csv("SalesOrderHeader"))
    df_sales_detail = drop_common_columns(load_csv("SalesOrderDetail"))
    df_product = drop_common_columns(load_csv("Product"))
    df_prod_cat = drop_common_columns(load_csv("ProductCategory"))
    df_prod_subcat = drop_common_columns(load_csv("ProductSubcategory"))
    df_sales_person = drop_common_columns(load_csv("SalesPerson"))
    df_sales_territory = drop_common_columns(load_csv("SalesTerritory"))
    df_unit_measure = drop_common_columns(load_csv("UnitMeasure"))

    df_customer.to_csv(f"{OUTPUT_PATH}/tmp_customer.csv", index=False)
    df_person.to_csv(f"{OUTPUT_PATH}/tmp_person.csv", index=False)
    df_sales_header.to_csv(f"{OUTPUT_PATH}/tmp_sales_header.csv", index=False)
    df_sales_detail.to_csv(f"{OUTPUT_PATH}/tmp_sales_detail.csv", index=False)
    df_product.to_csv(f"{OUTPUT_PATH}/tmp_product.csv", index=False)
    df_prod_cat.to_csv(f"{OUTPUT_PATH}/tmp_prod_cat.csv", index=False)
    df_prod_subcat.to_csv(f"{OUTPUT_PATH}/tmp_prod_subcat.csv", index=False)
    df_sales_person.to_csv(f"{OUTPUT_PATH}/tmp_sales_person.csv", index=False)
    df_sales_territory.to_csv(f"{OUTPUT_PATH}/tmp_sales_territory.csv", index=False)


def transform_data():
    df_customer = pd.read_csv(f"{OUTPUT_PATH}/tmp_customer.csv")
    df_person = pd.read_csv(f"{OUTPUT_PATH}/tmp_person.csv")
    df_sales_header = pd.read_csv(f"{OUTPUT_PATH}/tmp_sales_header.csv")
    df_sales_detail = pd.read_csv(f"{OUTPUT_PATH}/tmp_sales_detail.csv")
    df_product = pd.read_csv(f"{OUTPUT_PATH}/tmp_product.csv")
    df_prod_cat = pd.read_csv(f"{OUTPUT_PATH}/tmp_prod_cat.csv")
    df_prod_subcat = pd.read_csv(f"{OUTPUT_PATH}/tmp_prod_subcat.csv")
    df_sales_person = pd.read_csv(f"{OUTPUT_PATH}/tmp_sales_person.csv")
    df_sales_territory = pd.read_csv(f"{OUTPUT_PATH}/tmp_sales_territory.csv")

    df_sales_person = df_sales_person.rename(columns={"Name": "SalesPersonName"})
    df_sales_territory = df_sales_territory.rename(columns={"Name": "TerritoryName"})

    df_customer_full = df_customer.merge(
        df_person,
        how="left",
        left_on="PersonID",
        right_on="BusinessEntityID",
        suffixes=("", "_person")
    )

    df_sales = df_sales_detail.merge(
        df_sales_header,
        how="inner",
        on="SalesOrderID",
        suffixes=("", "_header")
    )

    df_product = df_product.merge(
        df_prod_subcat,
        how="left",
        on="ProductSubcategoryID"
    ).merge(
        df_prod_cat,
        how="left",
        on="ProductCategoryID"
    )

    df_sales = df_sales.merge(df_product, how="left", on="ProductID")
    df_sales_final = df_sales.merge(df_customer_full, how="left", on="CustomerID")
    df_sales_final = df_sales_final.merge(
        df_sales_person,
        how="left",
        left_on="SalesPersonID",
        right_on="BusinessEntityID"
    ).merge(
        df_sales_territory,
        how="left",
        on="TerritoryID"
    )

    
    df_sales_final.to_csv(f"{OUTPUT_PATH}/DW_SalesFact.csv", index=False)
    df_customer_full.to_csv(f"{OUTPUT_PATH}/DW_Customer.csv", index=False)
    df_product.to_csv(f"{OUTPUT_PATH}/DW_Product.csv", index=False)
    df_sales_person.to_csv(f"{OUTPUT_PATH}/DW_SalesPerson.csv", index=False)
    df_sales_territory.to_csv(f"{OUTPUT_PATH}/DW_SalesTerritory.csv", index=False)


def load_data():
    df_sales_final = pd.read_csv(f"{OUTPUT_PATH}/DW_SalesFact.csv")
    df_customer_full = pd.read_csv(f"{OUTPUT_PATH}/DW_Customer.csv")
    df_product = pd.read_csv(f"{OUTPUT_PATH}/DW_Product.csv")
    df_sales_person = pd.read_csv(f"{OUTPUT_PATH}/DW_SalesPerson.csv")
    df_sales_territory = pd.read_csv(f"{OUTPUT_PATH}/DW_SalesTerritory.csv")

    df_sales_final.to_sql("salesfact", engine, if_exists="replace", index=False)
    df_customer_full.to_sql("dim_customer", engine, if_exists="replace", index=False)
    df_product.to_sql("dim_product", engine, if_exists="replace", index=False)
    df_sales_person.to_sql("dim_salesperson", engine, if_exists="replace", index=False)
    df_sales_territory.to_sql("dim_territory", engine, if_exists="replace", index=False)

    print("ETL FINALIZADO COM SUCESSO!")


with DAG(
    dag_id="etl_adventureworks_dw",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
):
    extract_task = PythonOperator(
        task_id="extracao",
        python_callable=extract_data
    )

    transform_task = PythonOperator(
        task_id="transformacao",
        python_callable=transform_data
    )

    load_task = PythonOperator(
        task_id="carga",
        python_callable=load_data
    )

    extract_task >> transform_task >> load_task
