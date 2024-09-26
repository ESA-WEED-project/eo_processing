from eo_processing.utils.onnx_model_utilities import convert_catboost_model_to_onnx_with_metadata

# Define paths and features
catboost_model_path = 'H:\\slovakia\\models\\v5\\L1\\models\\65predictors\\v1\\catboost_v1.cbm'
output_onnx_path = 'C:\\Git_projects\\eo_processing\\src\\eo_processing\\resources\\test.onnx'

input_features = ['BLFEI_p98', 'NDVIMNDWI_mean', 'MNDWI_mean', 'B12_iqr', 'CIRE_p98',
                  'VHVVR_median', 'NDVI_iqr', 'NDMI_p2', 'VH_mean', 'VHVVR_sd', 'B05_p25',
                  'S2REP_p98', 'MNDWI_p98', 'VH_iqr', 'B07_p25', 'NDMI_sd', 'IRECI_p98',
                  'BLFEI_sd', 'NDVIMNDWI_median', 'B08_mean', 'B11_sd', 'S2WI_median',
                  'B02_p75', 'B06_p75', 'NIRv_sd', 'NIRv_mean', 'AVI_iqr', 'B05_p98',
                  'VHVVD_sd', 'NDWI_p2', 'B06_mean', 'B11_p98', 'VHVVR_iqr', 'B03_p75',
                  'RVI_mean', 'NDWI_p98', 'B8A_p98', 'VHVVD_mean', 'VH_p2', 'AVI_p75',
                  'S2REP_median', 'VHVVD_iqr', 'B03_sum', 'MNDWI_median', 'B06_median',
                  'VV_p75', 'AVI_p25', 'NIRv_p25', 'VH_p25', 'B12_sum', 'CIRE_p25',
                  'VHVVR_p75', 'B03_p2', 'B11_median', 'B05_median', 'S2WI_iqr', 'AVI_mean',
                  'IRECI_sum', 'B08_iqr', 'VHVVR_sum', 'IRECI_iqr', 'BLFEI_sum', 'S2WI_p2',
                  'B11_sum', 'B06_iqr']


output_features = ['predicted_label', 'prob_class_30000', 'prob_class_40000',
                   'prob_class_50000', 'prob_class_60000', 'prob_class_70000',
                   'prob_class_80000', 'prob_class_90000', 'prob_class_100000',
                   'prob_class_110000']  # Your output feature name(s)



convert_catboost_model_to_onnx_with_metadata(catboost_model_path, 
                                                 input_features, 
                                                 output_features, 
                                                 output_onnx_path)