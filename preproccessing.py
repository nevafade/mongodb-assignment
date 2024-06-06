#importing packages
import json
import csv


################################# converting units.csv to units.json ################################# 
csv_file = 'units.csv'
json_file = 'units.json'

#function to convert numerical values from string to integer
def is_float(value): #function to convert numerical values from string to integer
    try:
        int(value)  
        return True
    except ValueError:
        return False

try:
    with open(csv_file, mode='r', encoding='utf-8-sig') as file:
        #code for reading csv file
        csv_reader = csv.DictReader(file)
        #code for removing empty rows
        headers = csv_reader.fieldnames
        non_empty_headers = [header for header in headers if header]
        new_csv_reader = csv.DictReader(file, fieldnames=non_empty_headers,restkey='empty', restval='empty')
        data = []
        for row in new_csv_reader:
            if 'empty' in row:
                del row['empty'] #removing empty columns
            for key, value in row.items():
                if is_float(value):
                    row[key] = int(value) #converting numerical values to int
            data.append(row)

    with open(json_file, mode='w', encoding='utf-8') as json_out:
        #creating json file
        json.dump(data, json_out, indent=4)

    print(f"CSV data has been converted and saved to '{json_file}' as a JSON array.")
except FileNotFoundError:
    print(f"File '{csv_file}' not found.")
except Exception as e:
    print(f"An error occurred: {e}")

finally:
    ################################# converting students.csv to students.json #################################
    csv_file = 'students.csv'
    json_file = 'students.json'
    try:
        with open(csv_file, mode='r', encoding='utf-8-sig') as file:
            #code for reading csv file
            csv_reader = csv.DictReader(file)
            #code for removing empty columns
            headers = csv_reader.fieldnames
            non_empty_headers = [header for header in headers if header]
            new_csv_reader = csv.DictReader(file, fieldnames=non_empty_headers,restkey='empty', restval='empty')
            data = []
            for row in new_csv_reader:
                if 'empty' in row:
                    del row['empty'] #removing empty columns
                for key, value in row.items():
                    if is_float(value):
                        row[key] = int(value) #converting numerical values to int
                data.append(row)

        with open(json_file, mode='w', encoding='utf-8') as json_out:
            #creating json file
            json.dump(data, json_out, indent=4)

        print(f"CSV data has been converted and saved to '{json_file}' as a JSON array.")
    except FileNotFoundError:
        print(f"File '{csv_file}' not found.")
    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        ################################# Schema change - joining units to students using UnitCode #################################
        json_file = 'students.json'
        units_json = 'units.json'
        output_file = 'grouped_data.json'

    
        try:
            with open(json_file, 'r', encoding='utf-8') as file:
                #reading student data
                data = json.load(file)

            with open(units_json, 'r', encoding='utf-8') as file:
                #reading units data
                units_data = json.load(file)

            units_map = {} #creating map for unit data with unitcode is key

            for unit in units_data:
                units_map[unit['Code']] = unit
            grouped_data_arr = []


            for d in data:
                try:
                    d['course_details'] = units_map[d['UnitCode']] #creating a join
                except Exception as e:
                    print('see error')# error handling for units that dont exist in units table
            with open(output_file, 'w', encoding='utf-8') as json_out:
                #creating json file
                json.dump(data, json_out, indent=4)

            print(f"Grouped data has been saved to '{output_file}' as a JSON file.")

        except FileNotFoundError:
            print(f"File '{json_file}' not found.")
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")
        except Exception as e:
            print(f"An error occurred: {e}")
        finally:
            ################################# Schema change - creating relationship table for prohibitions and prerequisites #################################

            json_file = 'students.json'
            units_json = 'units.json'
            output_file = 'self_rel.json'  
            self_rel_data = []
            #Prerequisites
            #Prohibitions
            try:
                with open(units_json, 'r', encoding='utf-8') as file:
                    #reading units data
                    units_data = json.load(file)

                #creating table for prerequisites and prohibitions
                pacList=[]

                for unit in units_data:
                    Prerequisites = unit['Prerequisites']
                    Prohibitions = unit['Prohibitions']
                    #spliting prerequisites to make groups of units that share a OR relationship with each other
                    Prerequisites_arr = Prerequisites.upper().replace(" AND ","][").replace(" OR ",",").split("][")
                    Prerequisites_arr2 = []
                    for uc in Prerequisites_arr:
                        Prerequisites_arr2.append(uc.strip(' ().')) #triming the string
                    for uc in Prerequisites_arr2:
                        #creating a record for each ref code in Prerequisites
                        refCodes = uc.split(',')
                        for ref in refCodes:
                            pac={}
                            pac['refCode'] = ref #prerequisite code
                            pac['uc'] = uc #prerequisite group the refcode belongs to
                            pac['for'] = unit['Code'] #unit for which the prerequisite is made
                            pac['type'] = 'Requirement' #declaring the prerequsite relationship
                            pacList.append(pac)
                    #splitting prohibition units
                    Prohibitions_arr = Prohibitions.upper().replace(" AND ","][").replace(" OR ",",").split(",")
                    Prohibitions_arr2 = []
                    for uc in Prohibitions_arr:
                        Prohibitions_arr2.append(uc.strip(' ().')) #triming the string
                    for ref in Prohibitions_arr2:
                        pac={}
                        pac['refCode'] = ref #prohibited unit code
                        pac['uc'] = 'none' #there is no AND in prohibitions
                        pac['for'] = unit['Code'] #unit for which the prohibition is made
                        pac['type'] = 'Prohibition' #declaring the prohibition is made
                        pacList.append(pac)
                with open(output_file, 'w', encoding='utf-8') as json_out:
                    #create json files
                    json.dump(pacList, json_out, indent=4)

                print(f"Grouped data has been saved to '{output_file}' as a JSON file.")

            except FileNotFoundError:
                print(f"File '{json_file}' not found.")
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON: {e}")
            except Exception as e:
                print(f"An error occurred: {e}")
