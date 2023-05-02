from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression, Ridge, Lasso
from sklearn.svm import SVR
from xgboost import XGBRegressor
from catboost import CatBoostRegressor
from sklearn.metrics import mean_squared_error, r2_score


def train_linear_regression(X_train, y_train, X_test, y_test):
    model = LinearRegression()
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)
    return model, mean_squared_error(y_test, y_pred), r2_score(y_test, y_pred)


def train_ridge_regression(X_train, y_train, X_test, y_test):
    model = Ridge(alpha=1.0)
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)
    return model, mean_squared_error(y_test, y_pred), r2_score(y_test, y_pred)


def train_lasso_regression(X_train, y_train, X_test, y_test):
    model = Lasso(alpha=1.0)
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)
    return model, mean_squared_error(y_test, y_pred), r2_score(y_test, y_pred)


def train_svr(X_train, y_train, X_test, y_test):
    model = SVR(kernel='rbf')
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)
    return model, mean_squared_error(y_test, y_pred), r2_score(y_test, y_pred)


def train_xgboost(X_train, y_train, X_test, y_test):
    model = XGBRegressor()
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)
    return model, mean_squared_error(y_test, y_pred), r2_score(y_test, y_pred)


def train_catboost(X_train, y_train, X_test, y_test):
    model = CatBoostRegressor(verbose=False)
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)
    return model, mean_squared_error(y_test, y_pred), r2_score(y_test, y_pred)


def split_data(data):
    X = data.drop(['Date', 'Close', 'Change'], axis=1)
    y = data['Close']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=0)
    return X_train, y_train, X_test, y_test

