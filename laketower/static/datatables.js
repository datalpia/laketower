import DataTable from 'datatables.net-bs5'
import 'datatables.net-columncontrol-bs5'

export { DataTable }

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
    return new DataTable(
        tableId,
        {
            data: options.data,
            columns: options.columns,
            searching: true,
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

                if (type === 'string') {
                    colDef.columnControl = ['order', ['searchList', 'spacer', 'orderAsc', 'orderDesc', 'orderClear']]
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
