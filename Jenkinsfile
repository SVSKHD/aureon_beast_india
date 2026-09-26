// Aureon MCX - Jenkins deployment pipeline (Docker based, status API on host port 1250).
//
// Secrets come from Jenkins credentials only (never from the repository):
//   aureon-dhan-client-id      (secret text)   -> DHAN_CLIENT_ID
//   aureon-dhan-access-token   (secret text)   -> DHAN_ACCESS_TOKEN
//   aureon-discord-token       (secret text)   -> DISCORD_TOKEN          (optional)
//   aureon-discord-channel-id  (secret text)   -> DISCORD_CHANNEL_ID     (optional)
//   aureon-discord-webhook     (secret text)   -> deployment notifications (optional)
// Deployment fails closed when the required Dhan secrets are missing; values are masked in logs.
//
// Persistent research data survives container replacement through host volumes:
//   /data/aureon/sqlite  -> /data/sqlite   (SQLite DB, crash reports, incidents, events)
//   /data/aureon/parquet -> /data/parquet  (archive)
//   /data/aureon/logs    -> /data/logs
pipeline {
    agent any

    options {
        timestamps()
        disableConcurrentBuilds()
        buildDiscarder(logRotator(numToKeepStr: '20'))
        timeout(time: 40, unit: 'MINUTES')
    }

    environment {
        IMAGE_NAME      = 'aureon-beast-india'
        CONTAINER_NAME  = 'aureon-beast-india'
        API_PORT        = '1250'
        DATA_ROOT       = '/data/aureon'
        PYTHON          = 'python3'
    }

    stages {
        stage('Checkout') {
            steps {
                checkout scm
            }
        }

        stage('Resolve Git SHA') {
            steps {
                script {
                    env.GIT_SHA = sh(script: 'git rev-parse HEAD', returnStdout: true).trim()
                    env.GIT_SHORT = env.GIT_SHA.take(7)
                    env.GIT_BRANCH_NAME = sh(script: 'git rev-parse --abbrev-ref HEAD', returnStdout: true).trim()
                    env.IMAGE_TAG = "${env.IMAGE_NAME}:${env.GIT_SHA}"
                    env.APP_VERSION = sh(script: "grep -m1 '^version' pyproject.toml | sed 's/.*\"\\(.*\\)\".*/\\1/'", returnStdout: true).trim()
                    currentBuild.description = "${env.GIT_SHORT} on ${env.GIT_BRANCH_NAME}"
                }
            }
        }

        stage('Python syntax / static sanity') {
            steps {
                sh '''
                    set -eu
                    ${PYTHON} -m venv .venv
                    . .venv/bin/activate
                    python -m pip install --upgrade pip >/dev/null
                    python -m compileall -q aureon_mcx main_aureon.py
                    python -c "import ast,sys,pathlib; [ast.parse(p.read_text()) for p in pathlib.Path('aureon_mcx').rglob('*.py')]; print('syntax ok')"
                '''
            }
        }

        stage('Install dependencies') {
            steps {
                sh '''
                    set -eu
                    . .venv/bin/activate
                    pip install -r requirements.txt
                '''
            }
        }

        stage('Run pytest') {
            steps {
                sh '''
                    set -eu
                    . .venv/bin/activate
                    MPLBACKEND=Agg python -m pytest -q -p no:cacheprovider --junitxml=reports/pytest.xml
                '''
            }
            post {
                always {
                    junit allowEmptyResults: true, testResults: 'reports/pytest.xml'
                }
            }
        }

        stage('Build Docker image') {
            steps {
                sh '''
                    set -eu
                    docker build \
                      --build-arg GIT_SHA="${GIT_SHA}" \
                      --build-arg GIT_BRANCH="${GIT_BRANCH_NAME}" \
                      --build-arg BUILD_NUMBER="${BUILD_NUMBER}" \
                      --build-arg APP_VERSION="${APP_VERSION}" \
                      -t "${IMAGE_TAG}" -t "${IMAGE_NAME}:latest" .
                '''
            }
        }

        stage('Stop / replace previous container') {
            steps {
                sh '''
                    set -eu
                    # remember the running image so a failed health check can roll back
                    PREVIOUS=$(docker inspect --format '{{.Config.Image}}' "${CONTAINER_NAME}" 2>/dev/null || true)
                    echo "${PREVIOUS}" > .previous_image
                    if [ -n "${PREVIOUS}" ]; then
                        docker tag "${PREVIOUS}" "${IMAGE_NAME}:rollback" || true
                    fi
                    docker stop --time 30 "${CONTAINER_NAME}" 2>/dev/null || true
                    docker rm "${CONTAINER_NAME}" 2>/dev/null || true
                    mkdir -p "${DATA_ROOT}/sqlite" "${DATA_ROOT}/parquet" "${DATA_ROOT}/logs"
                '''
            }
        }

        stage('Start new container') {
            steps {
                withCredentials([
                    string(credentialsId: 'aureon-dhan-client-id', variable: 'DHAN_CLIENT_ID'),
                    string(credentialsId: 'aureon-dhan-access-token', variable: 'DHAN_ACCESS_TOKEN'),
                ]) {
                    script {
                        // optional Discord credentials: deploy headless when they are not configured
                        def discordToken = ''
                        def discordChannel = ''
                        try {
                            withCredentials([string(credentialsId: 'aureon-discord-token', variable: 'DT'),
                                             string(credentialsId: 'aureon-discord-channel-id', variable: 'DC')]) {
                                discordToken = env.DT
                                discordChannel = env.DC
                            }
                        } catch (ignored) {
                            echo 'Discord credentials not configured: deploying headless'
                        }
                        withEnv(["DISCORD_TOKEN=${discordToken}", "DISCORD_CHANNEL_ID=${discordChannel}"]) {
                            sh '''
                                set -eu
                                if [ -z "${DHAN_CLIENT_ID}" ] || [ -z "${DHAN_ACCESS_TOKEN}" ]; then
                                    echo "required Dhan credentials are missing: refusing to deploy" >&2
                                    exit 1
                                fi
                                # secrets are passed through the environment only (never on the image, never in logs)
                                docker run -d --name "${CONTAINER_NAME}" --restart unless-stopped \
                                  -p ${API_PORT}:1250 \
                                  -v "${DATA_ROOT}/sqlite:/data/sqlite" \
                                  -v "${DATA_ROOT}/parquet:/data/parquet" \
                                  -v "${DATA_ROOT}/logs:/data/logs" \
                                  -e DHAN_CLIENT_ID -e DHAN_ACCESS_TOKEN -e DISCORD_TOKEN -e DISCORD_CHANNEL_ID \
                                  -e AUREON_GIT_SHA="${GIT_SHA}" -e AUREON_GIT_BRANCH="${GIT_BRANCH_NAME}" -e AUREON_BUILD_NUMBER="${BUILD_NUMBER}" \
                                  -e AUREON_API_PORT=1250 -e AUREON_API_HOST=0.0.0.0 \
                                  "${IMAGE_TAG}"
                            '''
                        }
                    }
                }
            }
        }

        stage('Health check') {
            steps {
                sh '''
                    set -eu
                    ok=0
                    for i in $(seq 1 30); do
                        if curl -fsS "http://127.0.0.1:${API_PORT}/api/v1/health" > health.json 2>/dev/null; then
                            ok=1; break
                        fi
                        sleep 5
                    done
                    if [ "${ok}" != "1" ]; then
                        echo "health check FAILED after 150 s; container logs:" >&2
                        docker logs --tail 100 "${CONTAINER_NAME}" >&2 || true
                        exit 1
                    fi
                    cat health.json; echo
                    python3 - <<'PY'
import json
h = json.load(open("health.json"))
assert h.get("alive") is True, h
print("api alive; ready =", h.get("ready"), "; status =", h.get("status"))
PY
                '''
            }
        }

        stage('Deployment notification') {
            steps {
                script {
                    try {
                        withCredentials([string(credentialsId: 'aureon-discord-webhook', variable: 'DISCORD_WEBHOOK_URL')]) {
                            sh '''
                                . .venv/bin/activate
                                python -m aureon_mcx.tools.deploy_notify --status success --git "${GIT_SHORT}" --branch "${GIT_BRANCH_NAME}" \
                                  --version "${APP_VERSION}" --port "${API_PORT}" --health health.json --tests passed
                            '''
                        }
                    } catch (ignored) {
                        echo 'no Discord webhook credential configured: skipping deployment notification'
                    }
                }
            }
        }
    }

    post {
        failure {
            script {
                try {
                    withCredentials([string(credentialsId: 'aureon-discord-webhook', variable: 'DISCORD_WEBHOOK_URL')]) {
                        sh '''
                            . .venv/bin/activate 2>/dev/null || true
                            python3 -m aureon_mcx.tools.deploy_notify --status failure --git "${GIT_SHORT:-unknown}" \
                              --branch "${GIT_BRANCH_NAME:-unknown}" --version "${APP_VERSION:-unknown}" --port "${API_PORT}" \
                              --stage "${STAGE_NAME:-unknown}" || true
                        '''
                    }
                } catch (ignored) {
                    echo 'no Discord webhook credential configured'
                }
                // roll back to the previous image when the new container failed its health check
                sh '''
                    if [ -s .previous_image ] && docker image inspect "${IMAGE_NAME}:rollback" >/dev/null 2>&1; then
                        echo "rolling back to $(cat .previous_image)"
                        docker stop --time 30 "${CONTAINER_NAME}" 2>/dev/null || true
                        docker rm "${CONTAINER_NAME}" 2>/dev/null || true
                        echo "previous image retained as ${IMAGE_NAME}:rollback; restart it with the same docker run command"
                    fi
                '''
            }
        }
        always {
            sh 'rm -f health.json .previous_image || true'
        }
    }
}
