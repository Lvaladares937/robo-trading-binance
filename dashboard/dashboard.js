// Configuração
const API_URL = 'http://localhost:5000/api';
let priceChart = null;
let rsiChart = null;
let volumeChart = null;
let performanceChart = null;
let operacoesChart = null;
let currentPar = '';
let currentTimeframe = '4h';

// Aguardar o DOM carregar
document.addEventListener('DOMContentLoaded', function() {
    console.log('Dashboard profissional iniciado');
    carregarPares();
    carregarTodosDados();
    
    // Atualizar a cada 30 segundos
    setInterval(() => {
        carregarTodosDados();
        if (currentPar) carregarGrafico(currentPar);
    }, 30000);
    
    // Eventos dos selects
    document.getElementById('par-select')?.addEventListener('change', (e) => {
        currentPar = e.target.value;
        carregarGrafico(currentPar);
    });
    
    document.getElementById('timeframe-select')?.addEventListener('change', (e) => {
        currentTimeframe = e.target.value;
        if (currentPar) carregarGrafico(currentPar);
    });
});

// ==================== CARREGAR DADOS PRINCIPAIS ====================

async function carregarTodosDados() {
    try {
        await Promise.all([
            carregarResumo(),
            carregarEstados(),
            carregarOperacoes(),
            carregarAnalises(),
            carregarPerformance()
        ]);
        document.getElementById('last-update').textContent = new Date().toLocaleTimeString();
    } catch (error) {
        console.error('Erro ao carregar dados:', error);
    }
}

// Carregar resumo (cards)
async function carregarResumo() {
    try {
        const response = await fetch(`${API_URL}/resumo`);
        const data = await response.json();
        
        document.getElementById('capital-total').textContent = formatarMoeda(data.capital_total);
        document.getElementById('posicoes-abertas').textContent = data.posicoes_abertas;
        document.getElementById('total-compras').textContent = data.total_compras;
        document.getElementById('total-vendas').textContent = data.total_vendas;
        document.getElementById('operacoes-24h').textContent = data.operacoes_24h;
        
        const lucro = await calcularLucroTotal();
        document.getElementById('lucro-total').textContent = formatarMoeda(lucro);
    } catch (error) {
        console.error('Erro ao carregar resumo:', error);
    }
}

async function calcularLucroTotal() {
    try {
        const response = await fetch(`${API_URL}/performance`);
        const data = await response.json();
        return data.reduce((total, item) => total + (item.lucro || 0), 0);
    } catch (error) {
        return 0;
    }
}

// Carregar posições
async function carregarEstados() {
    try {
        const response = await fetch(`${API_URL}/estados`);
        const estados = await response.json();
        
        const tbody = document.getElementById('posicoes-body');
        if (!tbody) return;
        
        if (!estados || estados.length === 0) {
            tbody.innerHTML = '<tr><td colspan="6">Nenhuma posição encontrada</td></tr>';
            return;
        }
        
        tbody.innerHTML = estados.map(estado => `
            <tr onclick="carregarGrafico('${estado.par}')" style="cursor:pointer">
                <td>${estado.par}</td>
                <td>${estado.ativo}</td>
                <td>${estado.posicao ? '<span class="badge-aberta">ABERTA</span>' : '<span class="badge-fechada">FECHADA</span>'}</td>
                <td>${formatarMoeda(estado.preco_compra)}</td>
                <td>${estado.ativo_comprado.toFixed(8)}</td>
                <td>${formatarMoeda(estado.capital_operacional)}</td>
            </tr>
        `).join('');
        
    } catch (error) {
        console.error('Erro ao carregar estados:', error);
        const tbody = document.getElementById('posicoes-body');
        if (tbody) tbody.innerHTML = '<tr><td colspan="6">Erro ao carregar</td></tr>';
    }
}

// Carregar operações
async function carregarOperacoes() {
    try {
        const response = await fetch(`${API_URL}/operacoes?limit=20`);
        const operacoes = await response.json();
        
        const tbody = document.getElementById('operacoes-body');
        if (!tbody) return;
        
        if (!operacoes || operacoes.length === 0) {
            tbody.innerHTML = '<tr><td colspan="7">Nenhuma operação encontrada</td></tr>';
            return;
        }
        
        tbody.innerHTML = operacoes.map(op => `
            <tr>
                <td>${formatarData(op.data_hora)}</td>
                <td>${op.par}</td>
                <td class="${op.tipo === 'COMPRA' ? 'text-green' : 'text-red'}">${op.tipo}</td>
                <td>${formatarMoeda(op.preco)}</td>
                <td>${op.quantidade.toFixed(8)}</td>
                <td>${formatarMoeda(op.valor_total)}</td>
                <td>${op.motivo || '-'}</td>
            </tr>
        `).join('');
        
    } catch (error) {
        console.error('Erro ao carregar operações:', error);
        const tbody = document.getElementById('operacoes-body');
        if (tbody) tbody.innerHTML = '<tr><td colspan="7">Erro ao carregar</td></tr>';
    }
}

// Carregar análises
async function carregarAnalises() {
    try {
        const response = await fetch(`${API_URL}/analises?limit=30`);
        const analises = await response.json();
        
        const tbody = document.getElementById('analises-body');
        if (!tbody) return;
        
        if (!analises || analises.length === 0) {
            tbody.innerHTML = '<tr><td colspan="7">Nenhuma análise encontrada</td></tr>';
            return;
        }
        
        tbody.innerHTML = analises.map(analise => {
            const sinalClass = analise.sinal_compra ? 'badge-compra' : 'badge-fechada';
            const sinalText = analise.sinal_compra ? 'COMPRAR' : 'AGUARDAR';
            
            return `
                <tr onclick="carregarGrafico('${analise.par}')" style="cursor:pointer">
                    <td>${formatarData(analise.timestamp)}</td>
                    <td>${analise.par}</td>
                    <td>${analise.score_total ? analise.score_total.toFixed(1) : '-'}</td>
                    <td>${analise.tendencia_macro || '-'}</td>
                    <td>${analise.direcao_4h || '-'}</td>
                    <td>${analise.padrao_detectado || '-'}</td>
                    <td><span class="${sinalClass}">${sinalText}</span></td>
                </tr>
            `;
        }).join('');
        
    } catch (error) {
        console.error('Erro ao carregar análises:', error);
        const tbody = document.getElementById('analises-body');
        if (tbody) tbody.innerHTML = '<tr><td colspan="7">Erro ao carregar</td></tr>';
    }
}

// Carregar performance e criar gráficos
async function carregarPerformance() {
    try {
        const response = await fetch(`${API_URL}/performance`);
        const performance = await response.json();
        criarGraficoPerformance(performance);
        criarGraficoOperacoes(performance);
    } catch (error) {
        console.error('Erro ao carregar performance:', error);
    }
}

// ==================== GRÁFICOS DE PERFORMANCE ====================

function criarGraficoPerformance(performance) {
    const ctx = document.getElementById('performance-chart');
    if (!ctx) return;
    
    const sorted = [...performance].sort((a, b) => b.lucro - a.lucro).slice(0, 8);
    const labels = sorted.map(p => p.par);
    const lucros = sorted.map(p => p.lucro);
    const cores = lucros.map(l => l >= 0 ? 'rgba(0, 255, 0, 0.7)' : 'rgba(255, 0, 0, 0.7)');
    
    if (window.performanceChart) window.performanceChart.destroy();
    
    window.performanceChart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [{
                label: 'Lucro/Prejuízo (R$)',
                data: lucros,
                backgroundColor: cores,
                borderColor: cores.map(c => c.replace('0.7', '1')),
                borderWidth: 1
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            plugins: {
                legend: { labels: { color: '#fff' } },
                tooltip: { callbacks: { label: (ctx) => `${ctx.dataset.label}: ${formatarMoeda(ctx.raw)}` } }
            },
            scales: {
                y: { ticks: { color: '#fff' }, grid: { color: 'rgba(255,255,255,0.1)' } },
                x: { ticks: { color: '#fff', rotation: 45 }, grid: { color: 'rgba(255,255,255,0.1)' } }
            }
        }
    });
}

function criarGraficoOperacoes(performance) {
    const ctx = document.getElementById('operacoes-chart');
    if (!ctx) return;
    
    const totalCompras = performance.reduce((sum, p) => sum + (p.compras || 0), 0);
    const totalVendas = performance.reduce((sum, p) => sum + (p.vendas || 0), 0);
    
    if (window.operacoesChart) window.operacoesChart.destroy();
    
    window.operacoesChart = new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: ['Compras', 'Vendas'],
            datasets: [{
                data: [totalCompras, totalVendas],
                backgroundColor: ['rgba(0, 255, 0, 0.7)', 'rgba(255, 0, 0, 0.7)'],
                borderColor: ['#00ff00', '#ff0000'],
                borderWidth: 1
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            plugins: {
                legend: { labels: { color: '#fff' } },
                tooltip: {
                    callbacks: {
                        label: (ctx) => {
                            const total = totalCompras + totalVendas;
                            const percentual = total > 0 ? (ctx.raw / total * 100).toFixed(1) : 0;
                            return `${ctx.label}: ${ctx.raw} (${percentual}%)`;
                        }
                    }
                }
            }
        }
    });
}

// ==================== GRÁFICO PRINCIPAL ====================

async function carregarPares() {
    try {
        const response = await fetch(`${API_URL}/estados`);
        const estados = await response.json();
        
        const select = document.getElementById('par-select');
        if (select && estados.length > 0) {
            select.innerHTML = estados.map(e => `<option value="${e.par}">${e.par}</option>`).join('');
            currentPar = estados[0].par;
            carregarGrafico(currentPar);
        }
    } catch (error) {
        console.error('Erro ao carregar pares:', error);
    }
}

async function carregarGrafico(par) {
    if (!par) return;
    
    try {
        console.log(`Carregando ${par} - ${currentTimeframe}`);
        
        const infoDiv = document.getElementById('info-suporte');
        if (infoDiv) infoDiv.innerHTML = '<div class="loading">📊 Buscando dados da Binance...</div>';
        
        const response = await fetch(`${API_URL}/grafico/${par}?intervalo=${currentTimeframe}&limit=80`);
        const data = await response.json();
        
        if (data.erro) {
            if (infoDiv) infoDiv.innerHTML = `<div class="error">❌ ${data.erro}</div>`;
            return;
        }
        
        if (!data.dados || data.dados.length === 0) {
            if (infoDiv) infoDiv.innerHTML = '<div class="error">⚠️ Sem dados disponíveis</div>';
            return;
        }
        
        // Atualizar informações
        if (infoDiv) {
            infoDiv.innerHTML = `
                <div class="info-grid">
                    <div class="info-card">💰 Preço: <strong>${formatarMoeda(data.preco_atual)}</strong></div>
                    <div class="info-card">🛡️ Suporte: ${formatarMoeda(data.suporte)}</div>
                    <div class="info-card">⚡ Resistência: ${formatarMoeda(data.resistencia)}</div>
                    <div class="info-card">📊 Timeframe: ${currentTimeframe.toUpperCase()}</div>
                    <div class="info-card">📈 Velas: ${data.dados.length}</div>
                </div>
            `;
        }
        
        // Criar gráficos
        criarGraficoPreco(data);
        criarGraficoVolume(data);
        criarGraficoRSI(data);
        
    } catch (error) {
        console.error('Erro:', error);
        const infoDiv = document.getElementById('info-suporte');
        if (infoDiv) infoDiv.innerHTML = `<div class="error">❌ Erro: ${error.message}</div>`;
    }
}

function criarGraficoPreco(data) {
    const ctx = document.getElementById('candlestick-chart');
    if (!ctx) return;
    
    if (priceChart) priceChart.destroy();
    
    const labels = data.dados.map(d => new Date(d.time).toLocaleString());
    const closes = data.dados.map(d => d.close);
    const opens = data.dados.map(d => d.open);
    const highs = data.dados.map(d => d.high);
    const lows = data.dados.map(d => d.low);
    
    const datasets = [
        {
            label: 'Preço',
            data: closes,
            type: 'line',
            borderColor: '#00d2ff',
            borderWidth: 2,
            pointRadius: 0,
            fill: false,
            tension: 0.1
        }
    ];
    
    // Médias móveis
    if (data.indicadores.ma7 && data.indicadores.ma7.some(v => v !== null)) {
        datasets.push({
            label: 'MA 7',
            data: data.indicadores.ma7,
            type: 'line',
            borderColor: '#ffff00',
            borderWidth: 1.5,
            pointRadius: 0,
            fill: false
        });
    }
    
    if (data.indicadores.ma20 && data.indicadores.ma20.some(v => v !== null)) {
        datasets.push({
            label: 'MA 20',
            data: data.indicadores.ma20,
            type: 'line',
            borderColor: '#00ffff',
            borderWidth: 1.5,
            pointRadius: 0,
            fill: false
        });
    }
    
    priceChart = new Chart(ctx, {
        data: { labels: labels, datasets: datasets },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: { mode: 'index', intersect: false },
            plugins: {
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            const idx = context.dataIndex;
                            if (context.dataset.label === 'Preço') {
                                return [
                                    `Abertura: ${formatarMoeda(opens[idx])}`,
                                    `Máxima: ${formatarMoeda(highs[idx])}`,
                                    `Mínima: ${formatarMoeda(lows[idx])}`,
                                    `Fechamento: ${formatarMoeda(closes[idx])}`
                                ];
                            }
                            return `${context.dataset.label}: ${formatarMoeda(context.raw)}`;
                        }
                    }
                },
                legend: { position: 'top', labels: { color: '#fff', font: { size: 10 } } }
            },
            scales: {
                x: { ticks: { color: '#fff', rotation: 45, autoSkip: true, maxTicksLimit: 8 }, grid: { color: 'rgba(255,255,255,0.1)' } },
                y: { ticks: { color: '#fff', callback: (v) => formatarMoeda(v) }, grid: { color: 'rgba(255,255,255,0.1)' } }
            }
        }
    });
}

function criarGraficoVolume(data) {
    const ctx = document.getElementById('volume-chart');
    if (!ctx) return;
    
    if (volumeChart) volumeChart.destroy();
    
    const labels = data.dados.map(d => new Date(d.time).toLocaleString());
    const volumes = data.dados.map(d => d.volume);
    const cores = data.dados.map(d => d.close >= d.open ? 'rgba(0, 255, 0, 0.5)' : 'rgba(255, 0, 0, 0.5)');
    
    volumeChart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [{
                label: 'Volume',
                data: volumes,
                backgroundColor: cores,
                borderColor: cores.map(c => c.replace('0.5', '1')),
                borderWidth: 1
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { display: false } },
            scales: {
                x: { ticks: { display: false }, grid: { display: false } },
                y: { ticks: { color: '#fff' }, grid: { color: 'rgba(255,255,255,0.1)' } }
            }
        }
    });
}

function criarGraficoRSI(data) {
    const ctx = document.getElementById('rsi-chart');
    if (!ctx) return;
    
    if (rsiChart) rsiChart.destroy();
    
    const labels = data.dados.map(d => new Date(d.time).toLocaleString());
    const rsiValues = data.indicadores.rsi || [];
    
    rsiChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: labels,
            datasets: [{
                label: 'RSI (14)',
                data: rsiValues,
                borderColor: '#00d2ff',
                backgroundColor: 'rgba(0, 210, 255, 0.1)',
                borderWidth: 2,
                pointRadius: 0,
                fill: true
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                tooltip: { callbacks: { label: (ctx) => `RSI: ${ctx.raw?.toFixed(2)}` } },
                legend: { labels: { color: '#fff' } }
            },
            scales: {
                x: { ticks: { display: false }, grid: { display: false } },
                y: { min: 0, max: 100, ticks: { color: '#fff' }, grid: { color: 'rgba(255,255,255,0.1)' } }
            }
        }
    });
}

// ==================== UTILITÁRIOS ====================

function formatarMoeda(valor) {
    if (valor === undefined || valor === null) return 'R$ 0,00';
    return `R$ ${Number(valor).toFixed(2).replace('.', ',')}`;
}

function formatarData(dataStr) {
    if (!dataStr) return '-';
    return new Date(dataStr).toLocaleString('pt-BR');
}

// Funções globais
window.carregarGrafico = carregarGrafico;
window.atualizarManual = carregarTodosDados;