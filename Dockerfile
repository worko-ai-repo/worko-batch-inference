FROM public.ecr.aws/lambda/python:3.11

COPY lambda_function_firestore_gcp.py ${LAMBDA_TASK_ROOT}

RUN pip install --no-cache-dir google-cloud-firestore google-cloud-storage google-auth

CMD ["lambda_function_firestore_gcp.lambda_handler"]
