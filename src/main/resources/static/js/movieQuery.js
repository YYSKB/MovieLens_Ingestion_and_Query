const BASE_URL = 'http://localhost:8081/api/v1/movie';
const DEFAULT_VISIBLE_ROWS = 10; // 默认显示的行数 (对于查询 2 和 3)

/**
 * 切换内容的折叠/展开状态
 */
function toggleFolding(collapsibleId, buttonId) {
    const collapsible = document.getElementById(collapsibleId);
    const button = document.getElementById(buttonId);

    const isCollapsed = collapsible.classList.toggle('collapsed');

    // 切换按钮图标
    if (isCollapsed) {
        button.classList.remove('collapse-icon');
        button.classList.add('expand-icon');
    } else {
        button.classList.remove('expand-icon');
        button.classList.add('collapse-icon');
    }
}

/**
 * 将 JSON 数据渲染为 HTML 表格
 * @param {Object[] | Object} data - 结果数据 (List<Map> 或 Map)
 * @param {string} containerId - 渲染表格的容器 ID
 * @param {boolean} isLargeData - 是否为大量数据 (查询 2 和 3)
 */
function renderTable(data, containerId, isLargeData = false) {
    const container = document.getElementById(containerId);
    container.innerHTML = '';

    if (!data || (Array.isArray(data) && data.length === 0)) {
        container.innerHTML = '<p class="error">无数据或查询结果为空。</p>';
        return;
    }

    let dataArray = Array.isArray(data) ? data : [data]; // 统一处理为数组

    // 计算平均分（仅对查询2和3有效，使用原始样式）
    let averageHtml = '';
    if (isLargeData && dataArray.length > 0 && dataArray[0].hasOwnProperty('rating')) {
        let totalRating = 0;
        let validCount = 0;

        // 累加评分
        dataArray.forEach(item => {
            const rating = parseFloat(item.rating);
            if (!isNaN(rating)) {
                totalRating += rating;
                validCount++;
            }
        });

        // 计算平均分并保留一位小数，使用原始文本样式（不新增样式类）
        const average = validCount > 0 ? (totalRating / validCount).toFixed(1) : '0.0';
        averageHtml = `<p style="font-weight: bold; margin-bottom: 10px;">平均分: ${average} (共${validCount}条评分)</p>`;
    }

    const keys = Object.keys(dataArray[0]);
    let html = '<table class="data-table"><thead><tr>';

    // 生成表头
    keys.forEach(key => {
        // 简单格式化 key
        const formattedKey = key.charAt(0).toUpperCase() + key.slice(1).replace(/([A-Z])/g, ' $1');
        html += `<th>${formattedKey}</th>`;
    });
    html += '</tr></thead><tbody>';

    // 生成表行
    dataArray.forEach((item, index) => {
        html += '<tr>';
        keys.forEach(key => {
            html += `<td>${item[key]}</td>`;
        });
        html += '</tr>';
    });

    html += '</tbody></table>';

    // 拼接平均分和表格HTML（保持原始样式）
    container.innerHTML = averageHtml + html;

    // 处理大量数据折叠逻辑
    if (isLargeData) {
        const tableElement = container.querySelector('.data-table');
        const toggleWrapper = container.closest('.result-box').querySelector('.floating-toggle');

        if (dataArray.length > DEFAULT_VISIBLE_ROWS) {
            // 如果结果超过默认行数，则显示折叠按钮
            toggleWrapper.classList.remove('hidden');

            const collapsibleId = container.closest('.collapsible-wrapper').id;
        } else {
            toggleWrapper.classList.add('hidden');
        }
    }
}

/**
 * 通用 Fetch 函数
 */
async function fetchData(endpoint, tableContainerId, collapsibleId, toggleId, isLargeData = false) {
    const container = document.getElementById(tableContainerId);
    const toggleButton = toggleId ? document.getElementById(toggleId) : null;
    const toggleWrapper = toggleButton ? toggleButton.closest('.floating-toggle') : null;

    container.innerHTML = "正在查询中，请稍候...";
    if (toggleWrapper) {
        toggleWrapper.classList.add('hidden');
    }

    try {
        const response = await fetch(endpoint);
        const data = await response.json();

        if (response.ok) {
            renderTable(data, tableContainerId, isLargeData);

            // 确保结果区域默认是展开的 (如果之前是折叠状态)
            if (collapsibleId) {
                document.getElementById(collapsibleId).classList.remove('collapsed');
                document.getElementById(toggleId).classList.remove('expand-icon');
                document.getElementById(toggleId).classList.add('collapse-icon');
            }
        } else {
            const errorMsg = `查询失败! HTTP 状态码: ${response.status} - ${data.message || '未知错误'}`;
            container.innerHTML = `<p class="error">${errorMsg}</p>`;
        }
    } catch (error) {
        const errorMsg = `网络错误或服务器无响应: ${error.message}`;
        container.innerHTML = `<p class="error">${errorMsg}</p>`;
    }
}

// 接口 1: 查询电影详情 (Map -> 单行表格)
function queryMovieDetail() {
    const title = document.getElementById('movieTitleInput').value.trim();
    if (!title) {
        alert("请输入电影名称！");
        return;
    }
    const endpoint = `${BASE_URL}/detail?title=${encodeURIComponent(title)}`;
    fetchData(endpoint, 'resultDetailTable', null, null, false);
}

// 接口 2: 查询用户评分 (List<Map> -> 表格 + 折叠)
function queryUserRatings() {
    const userId = document.getElementById('userIdInput').value.trim();
    if (!userId) {
        alert("请输入用户 ID！");
        return;
    }
    const endpoint = `${BASE_URL}/userRatings?userId=${encodeURIComponent(userId)}`;
    fetchData(endpoint, 'resultUserRatingsTable', 'userRatingsCollapsible', 'userRatingsToggleButton', true);
}

// 接口 3: 查询某电影所有评分 (List<Map> -> 表格 + 折叠)
function queryMovieAllRatings() {
    const title = document.getElementById('movieTitleRatingsInput').value.trim();
    if (!title) {
        alert("请输入电影名称！");
        return;
    }
    const endpoint = `${BASE_URL}/allRatings?title=${encodeURIComponent(title)}`;
    fetchData(endpoint, 'resultAllRatingsTable', 'allRatingsCollapsible', 'allRatingsToggleButton', true);
}
