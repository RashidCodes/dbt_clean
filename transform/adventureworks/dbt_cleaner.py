from dbt.cli.main import dbtRunner, dbtRunnerResult
from snowflake.snowpark.session import Session
from snowflake.snowpark import DataFrame
from colorama import Fore, Style
from art import tprint
import argparse
import json
import yaml
import os

def drop_model_from_snowflake(session: Session, model_name: str, model_type: str) -> DataFrame:
    """
    Drop a model from snowflake

    :params session: Session
        A snowflake session

    :params model_name: str
        The name of a dbt model

    :params model_type: str
        The type of the model e.g. view/table

    :returns snowflake.snowpark.DataFrame
        Sql run result in a dataframe
    """

    sql = f"drop {model_type} if exists {model_name}"
    return session.sql(sql).collect()


def get_dbt_models_in_sf(session: Session, dbt_roles: list, target_db: str) -> DataFrame:
    """
    Check the information schema to find tables/views
    owned by dbt

    :params session: Session
        A snowflake session

    :params dbt_roles: list
        A list of dbt roles used to materialise dbt models

    :params target_db: str
        The name of the target database
    """
    dbt_roles_str = ", ".join([f"'{role}'" for role in dbt_roles])
    get_dbt_models_in_sf_sql = (
        f"select lower(concat(table_catalog, '.', table_schema, '.', table_name)) as model_name, "
        "lower(table_type) as table_type "
        f"from {target_db}.information_schema.tables "
        f"where lower(table_owner) in ({dbt_roles_str})")

    return session.sql(get_dbt_models_in_sf_sql).collect()


def get_dbt_models_and_materializations(dbt: dbtRunner, target_db: str) -> dict:
    """
    Extract the materialization of all models in your dbt project
    for a target db

    :params dbt: dbtRunner
        A dbt runner

    :params target_db: str
        A target database

    :returns model_configs: dict
        A dictionary of models and their materialization
    """
    # models refers to dbt models and seeds
    dbt_models = ["list", "-q", "--output", "json", "--resource-type", "model"]
    dbt_seeds = ["list", "-q", "--output", "json", "--resource-type", "seed"]

    models: dbtRunnerResult = dbt.invoke(dbt_models)
    seeds: dbtRunnerResult = dbt.invoke(dbt_seeds)
    print("\n")
    dbt_models_and_seeds = {}
    for model in models.result:
        # convert model to json
        model_json = json.loads(model)
        model_config = {
            # par example: adventureworks.marts.dim_address
            f'{target_db}.{model_json.get("config").get("schema")}.{model_json.get("name")}': model_json.get("config").get("materialized")
        }
        dbt_models_and_seeds = {**dbt_models_and_seeds, **model_config}

    for seed in seeds.result:
        # convert model to json
        seed_json = json.loads(seed)
        seed_config = {
            # par example: adventureworks.marts.dim_address
            f'{target_db}.{seed_json.get("config").get("schema")}.{seed_json.get("name")}': seed_json.get("config").get("materialized")
        }
        dbt_models_and_seeds = {**dbt_models_and_seeds, **seed_config}

    return dbt_models_and_seeds


def check_snowflake_creds(args: argparse.Namespace):
    """
    Check if the user has provided their snowflake credentials

    :params: Namespace
        Argparse Namespace

    :returns check_snowflake_creds: bool
        True if snowflake credentials have been provided else False
    """
    snowflake_username = os.getenv("SNOWFLAKE_USERNAME")
    snowflake_password = os.getenv("SNOWFLAKE_PASSWORD")
    snowflake_account =  os.getenv("SNOWFLAKE_ACCOUNT")
    snowflake_role = os.getenv("SNOWFLAKE_ROLE")
    snowflake_vars = [
        snowflake_username,
        snowflake_password,
        snowflake_account,
        snowflake_role
    ]
    sf_creds_available = all(snowflake_vars)
    return (sf_creds_available, snowflake_vars)




if __name__ == "__main__":
    tprint("dbt_clean.", font="bubblehead")
    parser = argparse.ArgumentParser(
                prog='dbt_cleaner',
                description='Remove unnecessary dbt models from snowflake after development'
            )

    parser.add_argument('-t', '--target', help="dbt target e.g. dev, prod", required=True)
    parser.add_argument('-p', '--project', help="dbt project name e.g. adventureworks", required=True)
    parser.add_argument('-r', '--roles',
                        nargs="+",
                        help="The list of roles that dbt uses to materialise models. Default is [dbt_funcrole]",
                        default=["dbt_funcrole"],
                        action='extend')
    parser.add_argument('--version', action='version', version='%(prog)s 1.0.0')
    args = parser.parse_args()


    TARGET = args.target
    PROJECT_NAME = args.project
    dbt_roles = args.roles

    # initialize dbt runner
    dbt = dbtRunner()

    with open('profiles.yml', 'r') as profiles:
        profiles_config = yaml.safe_load(profiles)
    target_db = profiles_config.get(f"{PROJECT_NAME}").get("outputs").get(f"{TARGET}").get("database")
    assert target_db is not None, "Specify a target database"

    # check snowflake creds
    sf_creds_available, snowflake_vars = check_snowflake_creds(args=args)
    if sf_creds_available is False:
        print("Provide the snowflake credentials for your dbt user")
        print("Read the help docs with python dbt_cleaner.py -h")
        exit(1)

    # snowflake connection
    snowflake_username, snowflake_password = snowflake_vars[:2]
    snowflake_account, snowflake_role = snowflake_vars[2:]
    connection_params = {
        "user": snowflake_username,
        "password": snowflake_password,
        "account": snowflake_account,
        "role": snowflake_role,
        "warehouse": "compute_etl",
        "database": target_db
    }

    # create snowflake session
    session = Session.builder.configs(connection_params).create()

    # get dbt models in snowflake
    # NB: dbt owns all objects created by dbt_funcrole role
    dbt_models_in_sf_df = get_dbt_models_in_sf(session, dbt_roles=dbt_roles, target_db=target_db)
    # dbt models in snowflake (cleaned)
    dbt_models_in_sf: dict = {row.MODEL_NAME: row.TABLE_TYPE for row in dbt_models_in_sf_df}

    # models in your current dbt project
    model_configs: dict = get_dbt_models_and_materializations(dbt=dbt, target_db=target_db)
    models_tracked_by_dbt = {key for key, _ in model_configs.items()}
    print(f"Running dbt clean with the following roles: {dbt_roles}")

    # find discrepancy
    model_names_in_sf = {model for model, _ in dbt_models_in_sf.items()}
    model_names_in_curr_dbt_proj = {dbt_model for dbt_model, _ in model_configs.items()}
    deleted_models = model_names_in_sf.difference(model_names_in_curr_dbt_proj)

    if len(deleted_models) > 0:
        print(f"{Fore.RED}The following tables will be dropped: {deleted_models}{Style.RESET_ALL}")
        drop_models = input("Enter 'yes' to drop models: ")

        if drop_models.lower() == "yes":
            # remove deleted models
            for deleted_model in deleted_models:
                print(f"Deleted model: {deleted_model}")
                model_type = dbt_models_in_sf.get(f"{deleted_model}")

                if "table" in model_type:
                    model_type_for_query = "table"
                if "view" in model_type:
                    model_type_for_query = "view"

                result = drop_model_from_snowflake(session=session, model_name=deleted_model, model_type=model_type_for_query)
                print(result[0].status)

            print("Successfully cleaned unused dbt models.")
            exit(0)
        print("dbt_cleanup cancelled")
        exit(0)

    print("No cleaning required!")
