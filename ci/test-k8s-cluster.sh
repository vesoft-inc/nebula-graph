#!/usr/bin/env bash
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

set -e -o pipefail

PROJ_DIR="$(cd "$(dirname "$0")" && pwd)/.."

kubectl get pods

kubectl delete sts.apps.nebula.io nebulaclusters-metad
kubectl delete sts.apps.nebula.io nebulaclusters-storaged
kubectl delete sts.apps.nebula.io nebulaclusters-graphd

sleep 15

while true; do
    status=$(kubectl get nebulaclusters -o jsonpath='{.items[0].status.conditions[?(@.type=="Ready")].status}')
    if [[ "$status" == "True" ]]; then
        break
    fi
    sleep 1
done

echo "Upgrade nebula cluster successfully."

kubectl delete job nebula-test || true

sleep 5 # wait graphd service starting
kubectl apply -f $PROJ_DIR/ci/nebula-test-job.yml
sleep 5 # wait nebula-test job dumping logs
kubectl logs -f jobs/nebula-test
