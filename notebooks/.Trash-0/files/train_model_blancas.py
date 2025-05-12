import os
import numpy as np
import tensorflow as tf
from sklearn.model_selection import train_test_split
from tensorflowonspark import TFCluster, TFNode

from pyspark.sql import SparkSession

os.environ["OMP_NUM_THREADS"] = "2"
os.environ["TF_NUM_INTRAOP_THREADS"] = "2"
os.environ["TF_NUM_INTEROP_THREADS"] = "1"

# Función que define la arquitectura del modelo
def model_fn(num_classes):
    model = tf.keras.Sequential([
        tf.keras.Input(shape=(8, 8, 12)),
        tf.keras.layers.Conv2D(64, (3, 3), activation='relu', padding='same'),
        tf.keras.layers.MaxPooling2D((2, 2)),
        tf.keras.layers.Conv2D(128, (3, 3), activation='relu', padding='same'),
        tf.keras.layers.MaxPooling2D((2, 2)),
        tf.keras.layers.Flatten(),
        tf.keras.layers.Dense(256, activation='relu'),
        tf.keras.layers.Dropout(0.3),
        tf.keras.layers.Dense(num_classes, activation='softmax')
    ])
    model.compile(optimizer='adam', loss='sparse_categorical_crossentropy', metrics=['accuracy'])
    return model

# Función principal que ejecuta cada nodo
def main_fun(args, ctx):
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    
    # Leer el parquet desde HDFS
    df_spark = spark.read.parquet(args["data_path"])
    df = df_spark.toPandas()  # Convertir a pandas DataFrame

    X = np.array(df["features"].tolist()).reshape(-1, 8, 8, 12)
    y = df["label"].astype(np.int32).values

    num_classes = len(np.unique(y))
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.1)

    model = model_fn(num_classes)
    model.fit(X_train, y_train, epochs=5, validation_data=(X_test, y_test), batch_size=32)

    if ctx.job_name == "chief":
        model.save(args["model_path"])

if __name__ == "__main__":
    spark = SparkSession.builder.appName("TFoS-CNN-Blancas").getOrCreate()
    sc = spark.sparkContext

    # Ruta al HDFS
    args = {
        "data_path": "hdfs:///user/ajedrez/datos_cnn/blancas/fen_jugadas.parquet",
        "model_path": "/notebooks/datos_cnn/modelo_blancas"
    }

    import socket
    args["host"] = socket.gethostbyname(socket.gethostname())

    cluster = TFCluster.run(
        sc, main_fun, args,
        num_executors=2,
        num_ps=1,
        input_mode=TFCluster.InputMode.TENSORFLOW,
        master_node="chief",
        log_dir="/tmp/tf_logs"
    )

    cluster.shutdown()
