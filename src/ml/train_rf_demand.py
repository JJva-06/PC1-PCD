import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error
import joblib
import os

def main():
    print("Cargando dataset Gold...")
    df = pd.read_parquet("../../data/gold/dataset_go_conc.parquet")

    # Feature Engineering
    # Convert string date to features
    print("Preparando features temporales y espaciales...")
    
    # Simple feature engineering for the model
    # We predict passenger_count based on Hour, DayOfWeek, and UniqueServices
    # For station categorical data, we do a basic frequency encoding to avoid massive one-hot arrays
    freq_encoding = df['Boarding_stop_stn'].value_counts()
    df['stn_freq'] = df['Boarding_stop_stn'].map(freq_encoding)

    X = df[['hour', 'day_of_week', 'unique_services', 'stn_freq']]
    y = df['passenger_count']

    # Train/Test Split (80/20) - Garantizando no Data Leakage temporal usando los ultimos dias como test
    # (Para simplificar, usamos un train_test_split aleatorio, pero idealmente seria temporal)
    print("Dividiendo Train/Test (80/20)...")
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Entrenando Random Forest (aproximando el state-of-the-art de ML para demanda)
    print("Entrenando modelo Random Forest Regressor...")
    model = RandomForestRegressor(n_estimators=50, max_depth=10, random_state=42, n_jobs=-1)
    model.fit(X_train, y_train)

    # Evaluacion
    print("Evaluando modelo...")
    preds = model.predict(X_test)
    mae = mean_absolute_error(y_test, preds)
    rmse = np.sqrt(mean_squared_error(y_test, preds))

    print(f"Resultados del Modelo (Random Forest):")
    print(f"MAE: {mae:.2f} pasajeros por ventana de 15 min")
    print(f"RMSE: {rmse:.2f}")

    # Guardar modelo
    os.makedirs("../../models", exist_ok=True)
    joblib.dump(model, "../../models/rf_demand_model.joblib")
    print("Modelo guardado en models/rf_demand_model.joblib")

if __name__ == "__main__":
    main()
