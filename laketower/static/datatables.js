import DataTable from 'datatables.net-bs5'
import 'datatables.net-columncontrol-bs5'

export { DataTable }

const SEARCHLIST_MAX_CARDINALITY = 25

export function columnarToArrays(columnar, columnNames) {
    const cols = columnNames.map(k => columnar[k])
    if (cols.length === 0) return []
    return cols[0].map((_, i) => cols.map(col => col[i]))
}

export function arrowTypesToDataTables(arrowTypes) {
    return arrowTypes.map(type => {
        const typeStr = type.toLowerCase()
        if (typeStr.includes('int') || typeStr.includes('float') || typeStr.includes('double') ||
            typeStr.includes('decimal') || typeStr.includes('numeric')) {
            return 'numeric'
        } else if (typeStr.includes('timestamp') || typeStr.includes('date')) {
            return 'date'
        } else {
            return 'string'
        }
    })
}

export function createDataTable(tableId, options = {}) {
    const colOffset = options.columnIndexOffset || 0
    const columnNames = options.columnNames || []
    const columnUniques = options.columnUniques || {}
    return new DataTable(
        tableId,
        {
            data: options.data,
            columns: options.columns,
            searching: true,
            footerCallback: function(row, data, start, end, display) {
                if (!row || !options.columnTypes) return
                const cells = row.cells
                options.columnTypes.forEach((type, i) => {
                    const cell = cells[i + colOffset]
                    if (!cell) return
                    if (type !== 'numeric') {
                        cell.textContent = '-'
                        return
                    }
                    let total = 0
                    display.forEach(rowIdx => {
                        const val = data[rowIdx][i]
                        if (val != null && val !== '' && !isNaN(val)) total += parseFloat(val)
                    })
                    cell.textContent = total
                })
            },
            layout: {
                topStart: null,
                topEnd: null,
                bottomStart: ['pageLength', 'info'],
                bottomEnd: 'paging',
            },
            columnControl: ['order', ['search', 'spacer', 'orderAsc', 'orderDesc', 'orderClear']],
            columnDefs: options.columnTypes ? options.columnTypes.map((type, index) => {
                const colDef = {
                    target: index + colOffset,
                    type: type === 'numeric' ? 'num' : type === 'date' ? 'date' : 'string-utf8',
                }

                const colName = columnNames[index]
                const uniques = columnUniques[colName]
                if (type === 'string' && uniques && uniques.length < SEARCHLIST_MAX_CARDINALITY) {
                    colDef.columnControl = ['order', [{ extend: 'searchList', options: uniques }, 'spacer', 'orderAsc', 'orderDesc', 'orderClear']]
                } else {
                    colDef.columnControl = ['order', ['search', 'spacer', 'orderAsc', 'orderDesc', 'orderClear']]
                }

                return colDef
            }) : [],
            ordering: {
                indicators: false,
            },
            order: [],
            paging: true,
            pageLength: 10,
            lengthMenu: [10, 25, 50, 100],
            deferRender: true,
            scrollX: true,
        }
    )
}
