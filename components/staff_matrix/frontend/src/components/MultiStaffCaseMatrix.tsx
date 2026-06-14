import {
  flexRender,
  getCoreRowModel,
  getSortedRowModel,
  useReactTable,
  type ColumnDef,
  type SortingState,
} from "@tanstack/react-table";
import { useVirtualizer } from "@tanstack/react-virtual";
import { ChevronDown, ChevronUp, ChevronsUpDown } from "lucide-react";
import { useEffect, useMemo, useRef, useState } from "react";
import type {
  MatrixComponentValue,
  MatrixFilters,
  MatrixPayload,
  MatrixSummary,
  Ticket,
} from "../types/ticket";
import { normalizeTicketLookup } from "../utils/ticketLookup";
import { CaseInfoPanel } from "./CaseInfoPanel";
import { MatrixFilterBar } from "./MatrixFilterBar";
import { MatrixHeader } from "./MatrixHeader";
import { StaffCell } from "./StaffCell";

const TICKET_COL_W = 200;
const PRIORITY_COL_W = 72;
const STATUS_COL_W = 96;
const STAFF_COL_W = 64;
const ROW_H = 44;
const HEADER_H = 48;

function matchesFilters(ticket: Ticket, filters: MatrixFilters): boolean {
  const q = filters.search.trim().toLowerCase();
  if (q && !ticket.id.toLowerCase().includes(q) && !ticket.caseLabel.toLowerCase().includes(q)) {
    return false;
  }
  if (filters.status !== "All" && ticket.displayStatus !== filters.status) return false;
  if (filters.priority !== "All" && ticket.priority !== filters.priority) return false;
  if (filters.staff !== "All" && !ticket.assignedStaff.includes(filters.staff)) return false;
  return true;
}

function buildSummary(tickets: Ticket[]): MatrixSummary {
  if (!tickets.length) {
    return {
      totalCases: 0,
      avgStaffPerCase: 0,
      topCollaborativeCaseId: "",
      topCollaborativeStaffCount: 0,
    };
  }
  let top = tickets[0];
  let staffSum = 0;
  for (const t of tickets) {
    const n = t.assignedStaff.length;
    staffSum += n;
    if (n > top.assignedStaff.length) top = t;
  }
  return {
    totalCases: tickets.length,
    avgStaffPerCase: staffSum / tickets.length,
    topCollaborativeCaseId: top.id,
    topCollaborativeStaffCount: top.assignedStaff.length,
  };
}

function SortIcon({ sorted }: { sorted: false | "asc" | "desc" }) {
  if (sorted === "asc") return <ChevronUp className="h-3 w-3" />;
  if (sorted === "desc") return <ChevronDown className="h-3 w-3" />;
  return <ChevronsUpDown className="h-3 w-3 opacity-40" />;
}

interface MultiStaffCaseMatrixProps {
  payload: MatrixPayload;
  height?: number;
  onLookupChange?: (value: MatrixComponentValue) => void;
}

export function MultiStaffCaseMatrix({
  payload,
  height = 720,
  onLookupChange,
}: MultiStaffCaseMatrixProps) {
  const { tickets, staffMembers, staffColors } = payload;
  const summary = payload.summary ?? buildSummary(tickets);

  const [filters, setFilters] = useState<MatrixFilters>(() => ({
    search: payload.lookupTicket ?? "",
    staff: "All",
    priority: "All",
    status: "All",
  }));
  const [sorting, setSorting] = useState<SortingState>([{ id: "id", desc: false }]);
  const [selectedId, setSelectedId] = useState<string | null>(null);

  const filteredTickets = useMemo(
    () => tickets.filter((t) => matchesFilters(t, filters)),
    [tickets, filters],
  );

  useEffect(() => {
    if (payload.lookupTicket && payload.lookupTicket !== filters.search) {
      setFilters((prev) => ({ ...prev, search: payload.lookupTicket ?? "" }));
    }
  }, [payload.lookupTicket, filters.search]);

  useEffect(() => {
    if (!onLookupChange) return undefined;
    const timer = window.setTimeout(() => {
      const trimmed = filters.search.trim();
      const normalized = normalizeTicketLookup(trimmed);
      const serverLookup = payload.lookupTicket ?? "";
      if (trimmed === "") {
        if (serverLookup !== "") {
          onLookupChange({ lookup: "" });
        }
        return;
      }
      if (normalized && normalized !== serverLookup) {
        onLookupChange({ lookup: normalized });
      }
    }, 450);
    return () => window.clearTimeout(timer);
  }, [filters.search, onLookupChange, payload.lookupTicket]);

  useEffect(() => {
    if (!filteredTickets.length) {
      setSelectedId(null);
      return;
    }
    if (!selectedId || !filteredTickets.some((t) => t.id === selectedId)) {
      setSelectedId(filteredTickets[0].id);
    }
  }, [filteredTickets, selectedId]);

  const columns = useMemo<ColumnDef<Ticket>[]>(() => {
    const base: ColumnDef<Ticket>[] = [
      {
        accessorKey: "caseLabel",
        id: "id",
        header: "Ticket ID",
        size: TICKET_COL_W,
        sortingFn: (a, b) => a.original.id.localeCompare(b.original.id),
        cell: ({ row }) => (
          <span
            className="truncate text-xs font-medium text-dashboard-text"
            title={row.original.id}
          >
            {row.original.caseLabel}
          </span>
        ),
      },
      {
        accessorKey: "priority",
        header: "Priority",
        size: PRIORITY_COL_W,
        cell: ({ getValue }) => {
          const p = String(getValue());
          const tone =
            p === "Critical" || p === "High"
              ? "font-semibold text-dashboard-accent"
              : "text-dashboard-muted";
          return <span className={`text-[11px] ${tone}`}>{p}</span>;
        },
      },
      {
        accessorKey: "displayStatus",
        header: "Status",
        size: STATUS_COL_W,
        cell: ({ getValue }) => (
          <span className="text-[11px] text-dashboard-muted">{String(getValue())}</span>
        ),
      },
    ];

    const staffCols: ColumnDef<Ticket>[] = staffMembers.map((staff) => ({
      id: `staff:${staff}`,
      accessorFn: (row) => row.staffAssignments[staff]?.outcome ?? null,
      header: () => {
        const short = staff.length > 12 ? `${staff.slice(0, 11)}…` : staff;
        const color = staffColors[staff] ?? "#9ec5e8";
        return (
          <span className="max-w-[60px] truncate text-[10px] font-medium normal-case" title={staff}>
            <span className="mr-1 inline-block h-1.5 w-1.5 rounded-full align-middle" style={{ backgroundColor: color }} />
            {short}
          </span>
        );
      },
      size: STAFF_COL_W,
      enableSorting: false,
      cell: ({ row }) => (
        <StaffCell
          assignment={row.original.staffAssignments[staff]}
          staffColor={staffColors[staff]}
          highlight={row.original.isShared && !!row.original.staffAssignments[staff]}
        />
      ),
    }));

    return [...base, ...staffCols];
  }, [staffMembers, staffColors]);

  const table = useReactTable({
    data: filteredTickets,
    columns,
    state: { sorting },
    onSortingChange: setSorting,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
  });

  const { rows } = table.getRowModel();
  const parentRef = useRef<HTMLDivElement>(null);
  const scrollStorageKey = "bon_perf_matrix_scroll_y";

  useEffect(() => {
    const el = parentRef.current;
    if (!el) return undefined;

    const saved = sessionStorage.getItem(scrollStorageKey);
    if (saved) {
      const y = Number(saved);
      if (Number.isFinite(y) && y > 0) {
        el.scrollTop = y;
      }
    }

    const onScroll = () => {
      sessionStorage.setItem(scrollStorageKey, String(el.scrollTop));
    };
    el.addEventListener("scroll", onScroll, { passive: true });
    return () => el.removeEventListener("scroll", onScroll);
  }, [scrollStorageKey]);

  const rowVirtualizer = useVirtualizer({
    count: rows.length,
    getScrollElement: () => parentRef.current,
    estimateSize: () => ROW_H,
    overscan: 16,
  });

  const virtualRows = rowVirtualizer.getVirtualItems();
  const totalWidth = TICKET_COL_W + PRIORITY_COL_W + STATUS_COL_W + staffMembers.length * STAFF_COL_W;

  const selectedTicket = useMemo(
    () => filteredTickets.find((t) => t.id === selectedId) ?? null,
    [filteredTickets, selectedId],
  );

  const selectTicket = (ticket: Ticket) => {
    setSelectedId(ticket.id);
  };

  const chromeH = 200;
  const gridBodyHeight = Math.max(200, height - chromeH);

  const viewEnd = virtualRows.length
    ? Math.min(
        filteredTickets.length,
        (virtualRows[virtualRows.length - 1]?.index ?? 0) + 1,
      )
    : 0;
  const viewStart = virtualRows.length ? (virtualRows[0]?.index ?? 0) + 1 : 0;

  return (
    <div
      className="flex flex-col overflow-hidden rounded-md border border-dashboard-border bg-black"
      style={{ height }}
    >
      <MatrixHeader summary={summary} />
      <MatrixFilterBar filters={filters} staffOptions={staffMembers} onChange={setFilters} />

      <div className="flex min-h-0 flex-1">
        <div className="flex min-w-0 flex-1 flex-col">
          <div
            ref={parentRef}
            className="min-h-0 flex-1 overflow-auto overscroll-y-contain bg-[#0a0a0a]"
            style={{ maxHeight: gridBodyHeight }}
          >
            <div style={{ minWidth: totalWidth }}>
              <div
                className="sticky top-0 z-30 flex border-b border-dashboard-border bg-[#161616] text-[10px] font-semibold uppercase tracking-wide text-dashboard-muted"
                style={{ height: HEADER_H }}
              >
                {table.getHeaderGroups().map((hg) =>
                  hg.headers.map((header) => {
                    const colId = header.column.id;
                    const isSticky =
                      colId === "id" || colId === "priority" || colId === "displayStatus";
                    const stickyLeft =
                      colId === "id"
                        ? 0
                        : colId === "priority"
                          ? TICKET_COL_W
                          : colId === "displayStatus"
                            ? TICKET_COL_W + PRIORITY_COL_W
                            : undefined;
                    const canSort = header.column.getCanSort();
                    return (
                      <div
                        key={header.id}
                        className={`flex shrink-0 items-center border-r border-dashboard-border/50 px-2 ${
                          isSticky ? "sticky z-40 bg-[#161616]" : "justify-center"
                        } ${canSort ? "cursor-pointer hover:text-dashboard-text" : ""}`}
                        style={{
                          width: header.getSize(),
                          left: stickyLeft,
                          boxShadow: isSticky ? "2px 0 8px rgba(0,0,0,0.45)" : undefined,
                        }}
                        onClick={canSort ? header.column.getToggleSortingHandler() : undefined}
                      >
                        <span className="flex items-center gap-1 truncate">
                          {flexRender(header.column.columnDef.header, header.getContext())}
                          {canSort ? <SortIcon sorted={header.column.getIsSorted()} /> : null}
                        </span>
                      </div>
                    );
                  }),
                )}
              </div>

              <div className="relative w-full" style={{ height: rowVirtualizer.getTotalSize() }}>
                {virtualRows.length === 0 ? (
                  <p className="px-4 py-10 text-center text-sm text-dashboard-muted">
                    No tickets match the current filters.
                  </p>
                ) : (
                  virtualRows.map((virtualRow) => {
                    const row = rows[virtualRow.index];
                    const ticket = row.original;
                    const isSelected = selectedId === ticket.id;
                    return (
                      <div
                        key={row.id}
                        data-index={virtualRow.index}
                        ref={rowVirtualizer.measureElement}
                        role="row"
                        tabIndex={0}
                        onClick={() => selectTicket(ticket)}
                        onKeyDown={(e) => {
                          if (e.key === "Enter" || e.key === " ") {
                            e.preventDefault();
                            selectTicket(ticket);
                          }
                        }}
                        className={`absolute left-0 flex w-full cursor-pointer border-b border-[#1f1f1f] text-xs transition ${
                          isSelected ? "bg-[#2a241c] ring-1 ring-inset ring-dashboard-accent/35" : "hover:bg-white/[0.02]"
                        }`}
                        style={{
                          height: ROW_H,
                          transform: `translateY(${virtualRow.start}px)`,
                        }}
                      >
                        {row.getVisibleCells().map((cell) => {
                          const colId = cell.column.id;
                          const isSticky =
                            colId === "id" || colId === "priority" || colId === "displayStatus";
                          const stickyLeft =
                            colId === "id"
                              ? 0
                              : colId === "priority"
                                ? TICKET_COL_W
                                : colId === "displayStatus"
                                  ? TICKET_COL_W + PRIORITY_COL_W
                                  : undefined;
                          return (
                            <div
                              key={cell.id}
                              role="cell"
                              className={`flex shrink-0 items-center border-r border-[#1f1f1f] px-2 ${
                                isSticky ? "sticky z-20 bg-[#0a0a0a]" : "justify-center"
                              } ${isSelected && isSticky ? "bg-[#1a1612]" : ""}`}
                              style={{
                                width: cell.column.getSize(),
                                left: stickyLeft,
                                boxShadow: isSticky ? "2px 0 6px rgba(0,0,0,0.3)" : undefined,
                              }}
                            >
                              {flexRender(cell.column.columnDef.cell, cell.getContext())}
                            </div>
                          );
                        })}
                      </div>
                    );
                  })
                )}
              </div>
            </div>
          </div>

          <footer className="flex items-center justify-end border-t border-dashboard-border bg-[#121212] px-4 py-2 text-[11px] text-dashboard-muted">
            {filteredTickets.length > 0 ? (
              <span>
                Viewing {viewStart}-{viewEnd} of {filteredTickets.length.toLocaleString()}
                {filteredTickets.length < tickets.length
                  ? ` (${tickets.length.toLocaleString()} total)`
                  : ""}
              </span>
            ) : (
              <span>No results</span>
            )}
          </footer>
        </div>

        <CaseInfoPanel
          ticket={selectedTicket}
          staffColors={staffColors}
          staffCount={selectedTicket?.assignedStaff.length ?? 0}
        />
      </div>
    </div>
  );
}
