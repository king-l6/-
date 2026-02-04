let conditionCounter = 0;

// 页面加载完成后初始化
document.addEventListener('DOMContentLoaded', function() {
    // 添加第一个条件
    addCondition();
    
    // 绑定表单提交事件
    document.getElementById('strategyForm').addEventListener('submit', handleSubmit);
});

// 添加条件
function addCondition() {
    const conditionsList = document.getElementById('conditionsList');
    const conditionId = `condition_${conditionCounter++}`;
    
    const conditionHtml = `
        <div class="condition-item" id="${conditionId}">
            <select class="condition-type" onchange="updateConditionInputs('${conditionId}')">
                <option value="limit_up">涨停</option>
                <option value="pct_change_gt">涨幅大于</option>
                <option value="pct_change_lt">涨幅小于</option>
                <option value="volume_ratio">成交量比例</option>
            </select>
            <input type="number" class="condition-date1" placeholder="交易日偏移(负数=往前推)" value="0">
            <input type="number" class="condition-value" placeholder="值" value="0" style="display:none;">
            <input type="number" class="condition-date2" placeholder="交易日偏移2" value="0" style="display:none;">
            <input type="number" class="condition-ratio" placeholder="比例" value="1" style="display:none;">
            <button type="button" class="btn-remove" onclick="removeCondition('${conditionId}')">删除</button>
        </div>
    `;
    
    conditionsList.insertAdjacentHTML('beforeend', conditionHtml);
    updateConditionInputs(conditionId);
}

// 更新条件输入框
function updateConditionInputs(conditionId) {
    const condition = document.getElementById(conditionId);
    const type = condition.querySelector('.condition-type').value;
    const valueInput = condition.querySelector('.condition-value');
    const date2Input = condition.querySelector('.condition-date2');
    const ratioInput = condition.querySelector('.condition-ratio');
    
    // 隐藏所有输入框
    valueInput.style.display = 'none';
    date2Input.style.display = 'none';
    ratioInput.style.display = 'none';
    
    // 根据类型显示相应的输入框
    if (type === 'pct_change_gt' || type === 'pct_change_lt') {
        valueInput.style.display = 'block';
    } else if (type === 'volume_ratio') {
        date2Input.style.display = 'block';
        ratioInput.style.display = 'block';
    }
}

// 删除条件
function removeCondition(conditionId) {
    document.getElementById(conditionId).remove();
}

// 加载示例策略
function loadExample() {
    // 清空现有条件
    document.getElementById('conditionsList').innerHTML = '';
    conditionCounter = 0;
    
    // 确保排除规则被选中（排除科创板、创业板、北交所、ST股、退市股）
    document.getElementById('excludeKCB').checked = true;  // 排除科创板
    document.getElementById('excludeCYB').checked = true;  // 排除创业板
    document.getElementById('excludeBJS').checked = true;   // 排除北交所
    document.getElementById('excludeST').checked = true;   // 排除ST股
    document.getElementById('excludeDelist').checked = true; // 排除退市股
    
    // 设置回测时间范围为近30个交易日
    document.getElementById('timeRange').value = '30';
    
    // 添加新策略条件（以回测日期为基准，负数表示往前推）
    // 假设回测日期是1月12日（base_date，偏移=0）
    // 1月07日是往前推3个交易日（自动跳过周末与节假日）
    addConditionWithValues('limit_up', -3, 0, 0, 1);
    // 1月08日往前推2个交易日，涨幅>0
    addConditionWithValues('pct_change_gt', -2, 0, 0, 0);
    // 1月09日往前推1个交易日，涨幅<0
    addConditionWithValues('pct_change_lt', -1, 0, 0, 0);
    // 1月08日成交量 / 1月09日成交量 > 1（-2 与 -1）
    addConditionWithValues('volume_ratio', -2, -1, 0, 1);
    // 1月12日成交量 / 1月09日成交量 > 1（0 与 -1）
    addConditionWithValues('volume_ratio', 0, -1, 0, 1);
    // 1月12日涨幅大于零：date1=0（1月12日，回测日期本身）
    addConditionWithValues('pct_change_gt', 0, 0, 0, 0);
}

function addConditionWithValues(type, date1, date2, value, ratio) {
    const conditionsList = document.getElementById('conditionsList');
    const conditionId = `condition_${conditionCounter++}`;
    
    const conditionHtml = `
        <div class="condition-item" id="${conditionId}">
            <select class="condition-type" onchange="updateConditionInputs('${conditionId}')">
                <option value="limit_up" ${type === 'limit_up' ? 'selected' : ''}>涨停</option>
                <option value="pct_change_gt" ${type === 'pct_change_gt' ? 'selected' : ''}>涨幅大于</option>
                <option value="pct_change_lt" ${type === 'pct_change_lt' ? 'selected' : ''}>涨幅小于</option>
                <option value="volume_ratio" ${type === 'volume_ratio' ? 'selected' : ''}>成交量比例</option>
            </select>
            <input type="number" class="condition-date1" placeholder="交易日偏移(负数=往前推)" value="${date1}">
            <input type="number" class="condition-value" placeholder="值" value="${value}" style="display:${type === 'pct_change_gt' || type === 'pct_change_lt' ? 'block' : 'none'};">
            <input type="number" class="condition-date2" placeholder="交易日偏移2" value="${date2}" style="display:${type === 'volume_ratio' ? 'block' : 'none'};">
            <input type="number" class="condition-ratio" placeholder="比例" value="${ratio}" style="display:${type === 'volume_ratio' ? 'block' : 'none'};">
            <button type="button" class="btn-remove" onclick="removeCondition('${conditionId}')">删除</button>
        </div>
    `;
    
    conditionsList.insertAdjacentHTML('beforeend', conditionHtml);
}

// 清空表单
function clearForm() {
    document.getElementById('conditionsList').innerHTML = '';
    conditionCounter = 0;
    addCondition();
    document.getElementById('resultsTable').innerHTML = '';
    document.getElementById('resultsInfo').innerHTML = '';
}

// 处理表单提交
async function handleSubmit(e) {
    e.preventDefault();
    
    // 收集条件
    const conditions = [];
    const conditionItems = document.querySelectorAll('.condition-item');
    
    conditionItems.forEach(item => {
        const type = item.querySelector('.condition-type').value;
        const date1 = parseInt(item.querySelector('.condition-date1').value) || 0;
        
        let condition = {
            type: type,
            date1: date1
        };
        
        if (type === 'pct_change_gt' || type === 'pct_change_lt') {
            const value = parseFloat(item.querySelector('.condition-value').value) || 0;
            condition.value = value;
        } else if (type === 'volume_ratio') {
            const date2 = parseInt(item.querySelector('.condition-date2').value) || 0;
            const ratio = parseFloat(item.querySelector('.condition-ratio').value) || 1;
            condition.date2 = date2;
            condition.ratio = ratio;
        }
        
        conditions.push(condition);
    });
    
    // 收集排除规则
    const exclude = {
        kcb: document.getElementById('excludeKCB').checked,
        cyb: document.getElementById('excludeCYB').checked,
        bjs: document.getElementById('excludeBJS').checked,
        st: document.getElementById('excludeST').checked,
        delist: document.getElementById('excludeDelist').checked
    };
    
    // 构建策略对象
    const strategy = {
        name: document.getElementById('strategyName')?.value || `策略_${new Date().toISOString().slice(0, 19).replace(/:/g, '-')}`,
        conditions: conditions,
        exclude: exclude,
        timeRange: parseInt(document.getElementById('timeRange').value)
    };
    
    // 显示加载状态
    document.getElementById('loading').style.display = 'block';
    document.getElementById('resultsTable').innerHTML = '';
    document.getElementById('resultsInfo').innerHTML = '';
    
    try {
        // 发送请求
        const response = await fetch('/api/backtest', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ strategy: strategy })
        });
        
        const result = await response.json();
        
        // 隐藏加载状态
        document.getElementById('loading').style.display = 'none';
        
        if (result.success) {
            displayResults(result.data, result.count);
        } else {
            document.getElementById('resultsInfo').innerHTML = `
                <div style="color: #ff4757; padding: 15px; background: #ffe0e0; border-radius: 6px;">
                    错误: ${result.error}
                </div>
            `;
        }
    } catch (error) {
        document.getElementById('loading').style.display = 'none';
        document.getElementById('resultsInfo').innerHTML = `
            <div style="color: #ff4757; padding: 15px; background: #ffe0e0; border-radius: 6px;">
                请求失败: ${error.message}
            </div>
        `;
    }
}

// 显示结果
function displayResults(data, count) {
    const resultsInfo = document.getElementById('resultsInfo');
    const resultsTable = document.getElementById('resultsTable');
    
    // 显示结果信息
    resultsInfo.innerHTML = `
        <div style="color: #667eea; font-weight: 600; font-size: 1.1em;">
            找到 ${count} 只符合条件的股票
        </div>
    `;
    
    if (count === 0) {
        resultsTable.innerHTML = '<div class="no-results">未找到符合条件的股票</div>';
        return;
    }
    
    // 构建表格
    let tableHtml = `
        <table>
            <thead>
                <tr>
                    <th>代码</th>
                    <th>名称</th>
                    <th>匹配日期</th>
                    <th>匹配价格</th>
                    <th>当前价格</th>
                    <th>涨跌幅</th>
                </tr>
            </thead>
            <tbody>
    `;
    
    data.forEach(stock => {
        const pctChange = stock.current_price && stock.match_price 
            ? ((stock.current_price - stock.match_price) / stock.match_price * 100).toFixed(2)
            : '-';
        const pctColor = parseFloat(pctChange) >= 0 ? '#28a745' : '#dc3545';
        
        tableHtml += `
            <tr>
                <td>${stock.code}</td>
                <td>${stock.name}</td>
                <td>${stock.match_date || '-'}</td>
                <td>${stock.match_price ? stock.match_price.toFixed(2) : '-'}</td>
                <td>${stock.current_price ? stock.current_price.toFixed(2) : '-'}</td>
                <td style="color: ${pctColor}; font-weight: 600;">${pctChange}%</td>
            </tr>
        `;
    });
    
    tableHtml += `
            </tbody>
        </table>
    `;
    
    resultsTable.innerHTML = tableHtml;
}
