# DBT Clean 
![dbt_clean.png](./assets/logo.png)

# Requirements
- Snowflake Account
- DBT Project

# Instructions
To remove unwanted materialised models from your snowflake account:

1. Copy and paste the `dbt_cleaner.py` script into your *dbt* project. You can find the script [here](./transform/adventureworks/dbt_cleaner.py)

1. Run the following command to find materialised models that can be removed. Use the `python dbt_cleaner.py -h` command for the cleaner's documentation.
    ```bash
    python dbt_cleaner.py --target prod --project adventureworks --roles dbt_funcrole finance_dbt_funcrole
    ```
1. Enter "yes" to remove unwanted models from snowflake

# Demo
1. Materialise your dbt models.
    ```bash
    # navigate to the adventureworks project
    cd ./transform/adventureworks
    dbt run --target prod
    ```

1. Rename the `report_sale` model in the *marts* directory to `report_sales`.
1. Run the cleaner.
    ```bash
    # dbt uses the listed roles to materialise the models
    python dbt_cleaner.py --target prod --project adventureworks --roles dbt_funcrole finance_dbt_funcrole
    ```

    ```bash
    Running dbt clean with the following roles: ['dbt_funcrole', 'finance_dbt_funcrole']
    The following tables will be dropped: {'adventureworks.marts.report_sale'}
    Enter 'yes' to drop models:
    ```

1. Type "yes" to remove the unwanted model from the target.
    ```bash
    Running dbt clean with the following roles: ['dbt_funcrole', 'finance_dbt_funcrole']
    The following tables will be dropped: {'adventureworks.marts.report_sale'}
    Enter 'yes' to drop models: yes
    Deleted model: adventureworks.marts.report_sale
    REPORT_SALE successfully dropped.
    Successfully removed unused dbt models.
    ```
1. Proceed to push your changes for inspection.

