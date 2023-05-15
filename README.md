# Moodle Log Data Temporal Dimension
This repository contains the template for calculating the temporal duration of Moodle log data based of their category. 
It takes as input the consolidated logs as described in [Moodle Log Data Consolidation](https://github.com/AD1529/Moodle-Log-Data-Consolidation) repository.

## Table of contents
* [Collect your data](#collect-your-data)
* * [Data structure](#data-structure)
* [Access your data](#access-your-data)
* [Get the categorical duration](#get-the-categorical-duration)
* [Get specific data](#get-specific-data)
* [License](#license)
* [Acknowledgments](#acknowledgements)
* [Contacts](#contact--s-)


## Quick start
This section contains a description of the data structure expected by the algorithms, as well as instructions on how 
collecting your data, accessing and consolidating them.
### Collect your data
The file is expected to be into *CSV* format.

#### Data structure
The input log file should contain at least the following columns:
- _ID_ - the sequence id
- _Time_ - the date and time of the action
- _Year_ - the year of the course. Remove this field if you are only analysing data from a specific year. 
- _Course_Area_ - the area of the platform or the course name
- _Unix_Time_ - the Unix timestamp
- _Username_ - the username of the user performing the action
- _Component_ - the module type (e.g., Wiki, Page, File, Url, Quiz)
- _Event_name_ - the type of action performed on the module (such as viewed, deleted, updated, created, and submitted)
- 'Role' - student, teacher, admin, course creator, guest, non-editing teacher
- _userid_ - the user id of the user performing the action
- _courseid_ - the course id where the action is performed
- 'Status' - status indicating whether the event was executed on a deleted activity or module


### Access your data
The *CSV* file should be placed in the `src/datasets/` folder. 
Replace path names in the `main.py` file. 

The `src/datasets` folder contains examples of the expected files. 

### Get the categorical duration
Make sure that you have all the necessary libraries, modules, and packages installed on your machine.
```bash
pip install -r requirements.txt
```
Run `main.py`.

According to your needs, you can also modify the `get_categorical_duration` function.

Following calculation, the dataset will contain the three additional columns:
- _basic_duration_ - the duration in seconds calculated as the difference between two consecutive timestamps
- _Category_ - the assigned category
- _categorical_duration_ - the corresponding duration based on the categories

You can clean the dataset by modifying functions in `src/algorithms/cleaning.py` file according to your needs. 

### Get specific data
Once the data has been consolidated, you can extract specific data by specifying the following parameters: 'year', 'course_area', 'role', 'username'. 
Note that you may choose more than one entry, and that each entry must be provided as a list.
The entire dataset is returned if you make no selections.

You can also specify the 'dates_path'  containing the course dates to remove values that don't fall within the start and 
end dates.
You can get start and end dates by querying the database:
```SQL
SELECT id, shortname, startdate, enddate 
FROM mdl_course
where id <> 1
```

#### Example

```bash
import src.algorithms.extracting as ex

# select specific attributes to get the desired values
COURSE_DATES_PATH = 'src/datasets/example_course_dates.csv'
course_A = ex.extract_records(records, course_area=['Course A'], role=['Student'], filepath=COURSE_DATES_PATH)
course_B = ex.extract_records(records, username=['Student 01'])
```
## License

This project is licensed under the terms of the GNU General Public License v3.0.

If you use the template in an academic setting, please cite the following paper:

> Rotelli, Daniela, and Anna Monreale. "Processing and Understanding Moodle Log Data and their Temporal Dimension", Journal of Learning Analytics, 2023

```tex
@article{rotelli2023processing,
  title={Processing and Understanding Moodle Log Data and their Temporal Dimension},
  author={Rotelli, Daniela and Monreale, Anna},
  booktitle={Journal of Learning Analytics},
  year={2023}
}
```


## Acknowledgements
This work has been partially supported by EU – Horizon 2020 Program under the scheme “INFRAIA-01-2018-2019 – Integrating 
Activities for Advanced Communities”, Grant Agreement n.871042, “SoBigData++: European Integrated Infrastructure for 
Social Mining and Big Data Analytics” (http://www.sobigdata.eu), the scheme "HORIZON-INFRA-2021-DEV-02 - Developing and 
consolidating the European research infrastructures landscape, maintaining global leadership (2021)", Grant Agreement 
n.101079043, “SoBigData RI PPP: SoBigData RI Preparatory Phase Project”, by NextGenerationEU - National Recovery and 
Resilience Plan (Piano Nazionale di Ripresa e Resilienza, PNRR) - Project: “SoBigData.it - Strengthening the Italian RI 
for Social Mining and Big Data Analytics” - Prot. IR0000013 - Avviso n. 3264 del 28/12/2021, and by PNRR - M4C2 - 
Investimento 1.3, Partenariato Esteso PE00000013 - ``FAIR - Future Artificial Intelligence Research" - Spoke 1 
"Human-centered AI", funded by the European Commission under the NextGeneration EU programme

## Contact(s)
[Daniela Rotelli](mailto:daniela.rotelli@phd.unipi.it) - Department of Computer Science - University of Pisa
