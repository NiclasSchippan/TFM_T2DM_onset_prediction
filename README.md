# TFM_T2DM_onset_prediction
Supportive material for the created TFM 

The supportive material is divided into R-Markdowns and Python-scripts.

The R-files contain one joined file for the creation of the datasets called "Section_preparation". Within this, two csv-files are generated, passed to the respective Python-files, where the actual model input is generated and returned, and are then used for the final dataframe configurations.
The other 2 R-files contain the Cox proportional hazard investigations, for each approch respectively.

The Python-files contain in the subfolder "Data_preparation" the two used files for the actual model input generation, called "Final_Preparation_input_data_Hb" and "Final_Preparation_input_data_Hb_trend" for both of the approaches. They make use of generated functions saved in "utils".
Furthermore the Python-files contain two identical subfolders for both approaches, where neural-network-based investigations are employed. Each subfolder contains two files, dedicated to parameter configurations and feature engineering respectively.
