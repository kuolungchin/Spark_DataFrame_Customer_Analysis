# Spark DataFrame Customer Analysis

This project demonstrates how to create a spark job and craete Spark Data Frame.

This source code is written in Scala.

It consists a few components:

1. Spark Context Factory
2. Job Component
3. Predefined Data Frame Schema Structure
4. Row Conversion Component
5. Implicit Field Value Converter via catching method of Exception utility:
     The converted field value will be wrapped in Option type object.
     Valid converted value will be subtype Some of Option;
     Otherwise, the value will be None.
6. Data Frame Getter

Note: We can have new data frame getters added into this project and operate multiple data frame object together
      in this project structure.
