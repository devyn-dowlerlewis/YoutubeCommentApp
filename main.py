import os

import menu

cul_df, cul_comdf = menu.reset_memory(silent=True)

API_KEY = ""

while (user_input := input(menu.generate_prompt(len(cul_df), len(cul_comdf)))) != "0":
    if user_input == "1":
        os.system('cls')
        if API_KEY == "":
            API_KEY = input("Please Enter Your Youtube API Key: ")
        cul_df, cul_comdf = menu.scrape(cul_df, cul_comdf, API_KEY)
        input("Press any key to continue")

    elif user_input == "2":
        os.system('cls')
        if API_KEY == "":
            API_KEY = input("Please Enter Your Youtube API Key: ")
        cul_df, cul_comdf = menu.scrape_one(cul_df, cul_comdf, API_KEY)
        input("Press any key to continue")

    elif user_input == "3":
        os.system('cls')
        cul_df, cul_comdf = menu.download_to_memory(cul_df, cul_comdf)
        input("Press any key to continue")

    elif user_input == "4":
        os.system('cls')
        menu.print_data(cul_df, cul_comdf)
        input("Press any key to continue")

    elif user_input == "5":
        os.system('cls')
        cul_df, cul_comdf = menu.upload_videos_comments(cul_df, cul_comdf)
        input("Press any key to continue")

    elif user_input == "6":
        os.system('cls')
        cul_df, cul_comdf = menu.eliminate_duplicates(cul_df, cul_comdf, keep=True)
        input("Press any key to continue")

    elif user_input == "7":
        os.system('cls')
        cul_df, cul_comdf = menu.eliminate_dbclones(cul_df, cul_comdf)
        input("Press any key to continue")

    elif user_input == "8":
        os.system('cls')
        cul_df, cul_comdf = menu.reset_memory()
        input("Press any key to continue")

    elif user_input == "9":
        os.system('cls')
        menu.toggle_threading()
        input("Press any key to continue")

    elif user_input == "10":
        pass
        # os.system('cls')
        # while (user_input := input(menu.generate_analysis_prompt(len(cul_df), len(cul_comdf)))) != "0":
    else:
        os.system('cls')
        input(f"{user_input} IS DECIDEDLY NOT AN OPTION")

    os.system('cls')


os.system('cls')
print("Program Exited Successfully :(")