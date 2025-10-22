import DataTable from 'datatables.net-bs5'
import 'datatables.net-columncontrol-bs5'

export { DataTable }

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
    return new DataTable(
        tableId,
        {
            searching: true,
            scrollX: true,
            layout: {
                topStart: null,
                topEnd: null,
                bottomStart: null,
                bottomEnd: null,
            },
            columnControl: ['order', ['search', 'spacer', 'orderAsc', 'orderDesc', 'orderClear']],
            columnDefs: options.columnTypes ? options.columnTypes.map((type, index) => {
                const colDef = {
                    target: index,
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
            paging: false,
            scrollY: '400px',
        }
    )
}
