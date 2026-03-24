build-WorkoBatchInferenceUpdaterFunction:
	mkdir -p $(ARTIFACTS_DIR)
	cp lambda_function.py $(ARTIFACTS_DIR)/
	pip3 install -r requirements.txt -t $(ARTIFACTS_DIR)/ --upgrade

build-WorkoBatchInferenceFirestoreUpdaterFunction:
	mkdir -p $(ARTIFACTS_DIR)
	cp lambda_function_firestore.py $(ARTIFACTS_DIR)/
	pip3 install -r requirements.txt -t $(ARTIFACTS_DIR)/ --upgrade

build-WorkoBatchInferenceFirestoreGcpUpdaterFunction:
	mkdir -p $(ARTIFACTS_DIR)
	cp lambda_function_firestore_gcp.py $(ARTIFACTS_DIR)/
	pip3 install -r requirements.txt -t $(ARTIFACTS_DIR)/ --upgrade
