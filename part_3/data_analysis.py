import pandas as pd
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split, cross_val_score, learning_curve
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix
import matplotlib.pyplot as plt
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
        with engine.connect() as connection:
            query = """
            SELECT customer_id, total_orders, total_revenue, repeat_customer
            FROM customer_data
            """
            df = pd.read_sql(query, connection)
        return df
    except Exception as e:
        print(f"Error loading data: {e}")
        return None

df = load_data()

if df is not None and len(df) >= 100:
    X = df[['total_orders', 'total_revenue']]
    y = df['repeat_customer']

    # Split data into training and temp
    X_train, X_temp, y_train, y_temp = train_test_split(X, y, test_size=0.3, random_state=42)

    # Split temp into validation and test sets
    X_val, X_test, y_val, y_test = train_test_split(X_temp, y_temp, test_size=0.5, random_state=42)

    # the Logistic Regression model
    model = LogisticRegression(random_state=42, max_iter=1000, C=1.0)

    model.fit(X_train, y_train)

    # Check Training and Validation Accuracy
    train_accuracy = model.score(X_train, y_train)
    val_accuracy = model.score(X_val, y_val)
    print(f"Training Accuracy: {train_accuracy:.2f}")
    print(f"Validation Accuracy: {val_accuracy:.2f}")

    if val_accuracy >= 0.7:
        y_test_pred = model.predict(X_test)

        # model’s test accuracy
        test_accuracy = accuracy_score(y_test, y_test_pred)
        print(f"Test Accuracy: {test_accuracy:.2f}")

        # classification report for the test set
        print("Test Classification Report:")
        print(classification_report(y_test, y_test_pred))

        # confusion matrix
        print("Confusion Matrix:")
        print(confusion_matrix(y_test, y_test_pred))

    # Cross-validation to check model stability
    cv_scores = cross_val_score(model, X, y, cv=5)
    print(f"Cross-validation scores: {cv_scores}")
    print(f"Mean cross-validation score: {cv_scores.mean():.2f}")

    # Learning Curves for visualizing overfitting
    train_sizes, train_scores, val_scores = learning_curve(model, X, y, cv=5, scoring='accuracy')
    train_scores_mean = train_scores.mean(axis=1)
    val_scores_mean = val_scores.mean(axis=1)

    plt.plot(train_sizes, train_scores_mean, label='Training score')
    plt.plot(train_sizes, val_scores_mean, label='Validation score')
    plt.xlabel("Training Set Size")
    plt.ylabel("Accuracy")
    plt.title("Learning Curves")
    plt.legend()
    plt.show()

    # Experiment with regularization by adjusting C value
    print("Experimenting with stronger regularization (C=0.1):")
    model_regularized = LogisticRegression(random_state=42, max_iter=1000, C=0.1)
    model_regularized.fit(X_train, y_train)
    train_accuracy_regularized = model_regularized.score(X_train, y_train)
    val_accuracy_regularized = model_regularized.score(X_val, y_val)
    print(f"Training Accuracy with C=0.1: {train_accuracy_regularized:.2f}")
    print(f"Validation Accuracy with C=0.1: {val_accuracy_regularized:.2f}")

    print("Model Coefficients:")
    for feature, coef in zip(X.columns, model.coef_[0]):
        print(f"{feature}: {coef}")

else:
    print("Data loading failed or insufficient data for training (minimum 100 rows required).")
