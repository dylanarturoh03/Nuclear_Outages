const state = {
    table: 'outages',
    order_by: 'ASC',
    limit: 10,
    page: 1,
    sort_by: 'id' 
}

let filters = {}

function showToast(message, type = 'success') {
    const container = document.getElementById('toastContainer')

    const toast = document.createElement('div')
    toast.className = `toast toast-${type}`
    toast.innerText = message

    container.appendChild(toast)

    setTimeout(function() {
        toast.classList.add('toast-fade')
        setTimeout(function() {
            toast.remove()
        }, 500)
    }, 3000)
}

async function refreshData(){
    refreshBtn.disabled = true
    refreshBtn.innerText= 'Refreshing...'
    loadingOverlay.style.display = 'flex'

    try {
        const response = await fetch('/refresh')
        const data =  await response.json()

        if (!response.ok) {
            showToast(data.error, 'error')
        }

        loadData()

    } catch (error) {
        showToast('Network error. Check your connection.', 'error')
        console.log('Refresh failed:', error)
    } finally {
        refreshBtn.disabled = false
        refreshBtn.innerText = 'Refresh'
        loadingOverlay.style.display = 'none'
    }
}

function getType(value){
    if (!isNaN(value) && value !=='') return 'number'
    if (isNaN(value) && !isNaN(Date.parse(value))) return 'date'
    return 'str'
}

function buildPopover(col, type, val){
    if (type === 'number') {
        return `
            <p class="popover-title">${col}: </p>
            <p class='popover-value'>${val}</p>
            <div class="filter-operators">
                <button class="operator-btn" data-op="eq">=</button>
                <button class="operator-btn" data-op="gt">&gt;</button>
                <button class="operator-btn" data-op="lt">&lt;</button>
                <button class="operator-btn" data-op="gte">&gt;=</button>
                <button class="operator-btn" data-op="lte">&lt;=</button>
            </div>
        `
    }

    if (type === 'date') {
        return `
            <p class="popover-title">${col}: </p>
            <p class='popover-value'>${val}</p>
            <div class="filter-operators">
                <button class="operator-btn" data-op="eq">=</button>
                <button class="operator-btn" data-op="gt">after</button>
                <button class="operator-btn" data-op="lt">before</button>
                <button class="operator-btn" data-op="gte">on or after</button>
                <button class="operator-btn" data-op="lte">on or before</button>
            </div>
        `
    }

    if (type === 'str') {
        return `
            <p class="popover-title">${col}: </p>
            <p class='popeover-value'>${val}</p>
            <div class="filter-operators">
                <button class="operator-btn" data-op="eq">exact match</button>
            </div>
        `
    }
}

function renderTable(data) {
    if (data.length === 0) {
        document.getElementById('table-body').innerHTML = `
        <p style='margin-top: 16px;'>No results found.</p>
        `
        showToast('No matching data was found in DB.', 'error')
        return
    }

    const columns = Object.keys(data[0])

    const headers = columns.map(col => `
        <th class='table-header ${state.sort_by === col ? 'active-sort' : ''}'
            data-col='${col}'>
            ${col}
            <span class='sort-arrow'>
                ${state.sort_by == col ? (state.order_by === 'ASC' ? '↑' : '↓') : '↕'}
            </span>
        </th>
    `).join('')

    const rows = data.map((row, index) =>
        `<tr class='${index % 2 == 0 ? 'row-even' : 'row-odd'}'>
            ${columns.map(col => `<td data-col='${col}' data-val='${row[col]}'>${row[col]}</td>`).join('')}
        </tr>`
    ).join('')
 
    document.getElementById('table-body').innerHTML = `
        <table class='data-table'>
            <thead><tr>${headers}</tr></thead>
            <tbody>${rows}</tbody>
        </table>
    `

    const tableHeaders = document.querySelectorAll('.table-header')

    tableHeaders.forEach(th => {
        th.addEventListener('click', function() {
            const col = this.dataset.col
            console.log(col)
            console.log(state.order_by, state.sort_by)
            if (state.sort_by == col){
                state.order_by = state.order_by === 'ASC' ? 'DESC' : 'ASC'
            } else {
                state.sort_by = col
                state.order_by = 'ASC'
            }
            loadData(true)
        })
    })

    const tableCells = document.querySelectorAll('td')

    tableCells.forEach(function(td) {
        td.addEventListener('click', function(event) {
            event.stopPropagation()

            const col = this.dataset.col
            const value = this.dataset.val
            const type = getType(value)

            popover.innerHTML = buildPopover(col, type, value)

            document.querySelectorAll('.operator-btn').forEach(function(btn) {
                btn.addEventListener('click', function(event) {
                    event.stopPropagation()

                    const op = this.dataset.op

                    if (!value) return

                    filters = {}
                    const filterKey = `${col}_${op}`
                    filters[filterKey] = value

                    popover.style.display = 'none'
                    loadData()
                })
            })

            popover.style.left = event.clientX + 'px'
            popover.style.top = event.clientY + 'px'
            popover.style.display = 'block'
        })
    })

    showToast('Data loaded correctly from DB', 'success')
}

async function loadData(keepPage=false){
    if (!keepPage){
        state.page = 1
    }

    const params = new URLSearchParams({
        ...state,
        ...filters
    })
    const response = await fetch(`/data?${params}`)

    if (!response.ok) {
        const error = await response.json()
        console.log(error)
        return
    }

    const data = await response.json()
    const curr_page = data.total_pages === 0 ? 0 : data.page
    document.getElementById('pageInfo').innerText = `Page ${curr_page} of ${data.total_pages}`

    // Update state of page buttons
    prevPage.disabled = curr_page <= 1
    nextPage.disabled = curr_page === data.total_pages

    renderTable(data.data)
}

const tabs = document.querySelectorAll('.tab')
const nextPage = document.getElementById('nextPage')
const prevPage = document.getElementById('prevPage')
const popover = document.getElementById('popover')
const refreshBtn = document.getElementById('refreshBtn')
const loadingOverlay = document.getElementById('loadingOverlay')

refreshBtn.addEventListener('click', refreshData)

document.addEventListener('click', function(event) {
    if(!popover.contains(event.target)){
        popover.style.display = 'none'
    }
})

tabs.forEach(function(btn) {
    btn.addEventListener('click', function() {
        tabs.forEach(b => b.classList.remove('active'))
        this.classList.add('active')
        state.table = this.dataset.table
        state.sort_by = 'id'
        filters = {}
        loadData()
    })
})

nextPage.addEventListener('click', function() {
    state.page++
    loadData(true)
})

prevPage.addEventListener('click', function() {
    state.page--
    loadData(true)
})

document.addEventListener('DOMContentLoaded', refreshData)
