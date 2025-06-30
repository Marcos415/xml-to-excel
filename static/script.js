// static/script.js

document.addEventListener('DOMContentLoaded', () => {
    const uploadForm = document.getElementById('uploadForm');
    const statusDiv = document.getElementById('status');
    const submitButton = uploadForm.querySelector('button[type="submit"]');

    uploadForm.addEventListener('submit', async function(event) {
        event.preventDefault(); // Impede o envio padrão do formulário

        const form = event.target;
        const formData = new FormData(form);

        // Resetar e mostrar status de processamento
        statusDiv.style.display = 'block';
        statusDiv.className = 'status-processing';
        statusDiv.textContent = 'Processando... Por favor, aguarde, isso pode levar alguns minutos.';
        submitButton.disabled = true; // Desabilita o botão enquanto processa

        try {
            const response = await fetch('/processar', {
                method: 'POST',
                body: formData
            });

            if (response.ok) {
                // Se a resposta for OK, significa que o arquivo Excel está pronto para download
                const blob = await response.blob();
                const url = window.URL.createObjectURL(blob);
                const a = document.createElement('a');
                a.href = url;

                // Tenta obter o nome do arquivo do header 'Content-Disposition'
                const contentDisposition = response.headers.get('Content-Disposition');
                let filename = 'resultado.xlsx'; // Nome padrão
                if (contentDisposition && contentDisposition.indexOf('attachment') !== -1) {
                    const filenameMatch = contentDisposition.match(/filename="([^"]+)"/);
                    if (filenameMatch && filenameMatch[1]) {
                        filename = filenameMatch[1];
                    }
                }
                a.download = filename; // Define o nome do arquivo para download
                document.body.appendChild(a);
                a.click(); // Simula um clique para iniciar o download
                a.remove(); // Remove o link temporário do DOM
                window.URL.revokeObjectURL(url); // Libera a URL do objeto Blob

                statusDiv.className = 'status-success';
                statusDiv.textContent = `Processamento concluído! O arquivo "${filename}" foi baixado.`;
            } else {
                // Se a resposta não for OK (ex: 400, 500), leia a mensagem de erro do servidor
                const errorText = await response.text();
                statusDiv.className = 'status-error';
                statusDiv.textContent = `Erro no processamento: ${errorText}`;
            }
        } catch (error) {
            // Erros de rede ou outros problemas no fetch
            statusDiv.className = 'status-error';
            statusDiv.textContent = `Erro de conexão ou servidor indisponível: ${error.message}`;
        } finally {
            submitButton.disabled = false; // Habilita o botão novamente, independentemente do resultado
        }
    });
});