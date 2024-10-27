import pandas as pd
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report
import warnings

DB_USER = 'root'
DB_PASSWORD = 'Muw0okm_PL<'
DB_HOST = 'localhost'
DB_PORT = '3306'
DB_NAME = 'delivery'

DATABASE_URL = f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

def load_data():
    try:
        engine = create_engine(DATABASE_URL)
        connection = engine.connect()

        query = """
        SELECT customer_id, total_orders, total_revenue, repeat_customer
        FROM customer_data
        """
        df = pd.read_sql(query, connection)

        connection.close()

        return df

    except Exception as e:
        print(f"Error loading data: {e}")
        return None

df = load_data()

if df is not None and len(df) >= 100:
    X = df[['total_orders', 'total_revenue']]
    y = df['repeat_customer']

    X_train, X_temp, y_train, y_temp = train_test_split(X, y, test_size=0.3, random_state=42)

    X_val, X_test, y_val, y_test = train_test_split(X_temp, y_temp, test_size=0.5, random_state=42)

    model = RandomForestClassifier(random_state=42)

    param_grid = {
        'n_estimators': [50, 100, 200],
        'max_depth': [None, 10, 20, 30],
        'min_samples_split': [2, 5, 10],
        'min_samples_leaf': [1, 2, 4],
        'max_features': ['sqrt', 'log2']
    }

    grid_search = GridSearchCV(estimator=model, param_grid=param_grid, cv=5, n_jobs=-1, scoring='accuracy', verbose=1)
    grid_search.fit(X_train, y_train)

    best_model = grid_search.best_estimator_
    print("Best Parameters:", grid_search.best_params_)

    y_val_pred = best_model.predict(X_val)
    val_accuracy = accuracy_score(y_val, y_val_pred)
    print(f"Validation Accuracy: {val_accuracy:.2f}")

    if val_accuracy >= 0.7:
        y_test_pred = best_model.predict(X_test)

        test_accuracy = accuracy_score(y_test, y_test_pred)
        print(f"Test Accuracy: {test_accuracy:.2f}")

        print("Test Classification Report:")
        print(classification_report(y_test, y_test_pred))
    else:
        warnings.warn("Validation accuracy is below the acceptable threshold. Consider revisiting the features or model.")
else:
    print("Data loading failed or insufficient data for training (minimum 100 rows required).")