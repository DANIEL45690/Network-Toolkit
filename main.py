import asyncio
import aiohttp
import websockets
import zeroconf
import dns.resolver
import subprocess
import sys
import os
import platform
import socket
import requests
import json
import re
import signal
from datetime import datetime
from typing import Dict, List, Optional, Any
from concurrent.futures import ThreadPoolExecutor

try:
    from colorama import init, Fore, Back, Style

    init(autoreset=True)
    HAS_COLORAMA = True
except ImportError:
    HAS_COLORAMA = False

    class Fore:
        RED = ""
        GREEN = ""
        YELLOW = ""
        BLUE = ""
        MAGENTA = ""
        CYAN = ""
        WHITE = ""
        RESET = ""

    class Style:
        BRIGHT = ""
        DIM = ""
        NORMAL = ""


running = True


def signal_handler(sig, frame):
    global running
    print(f"\n{Fore.YELLOW}[!] Ctrl+C pressed, shutting down...{Fore.RESET}")
    running = False
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)


def print_banner():
    banner = f"""
{Fore.CYAN}                      █████                                                  █████
                     ░░███                                                  ░░███
{Fore.BLUE} ████████    ██████  ███████              █████ ███ █████  ██████  ████████  ░███ █████
{Fore.MAGENTA}░░███░░███  ███░░███░░░███░    ██████████░░███ ░███░░███  ███░░███░░███░░███ ░███░░███
{Fore.CYAN} ░███ ░███ ░███████   ░███    ░░░░░░░░░░  ░███ ░███ ░███ ░███ ░███ ░███ ░░░  ░██████░
{Fore.BLUE} ░███ ░███ ░███░░░    ░███ ███            ░░███████████  ░███ ░███ ░███      ░███░░███
{Fore.MAGENTA} ████ █████░░██████   ░░█████              ░░████░████   ░░██████  █████     ████ █████
{Fore.CYAN}░░░░ ░░░░░  ░░░░░░     ░░░░░                ░░░░ ░░░░     ░░░░░░  ░░░░░     ░░░░ ░░░░░
{Fore.RESET}
"""
    print(banner)
    print(f"{Fore.YELLOW}{'=' * 70}{Fore.RESET}")
    print(
        f"{Fore.GREEN}Network Toolkit v3.0 - Type 'help' for commands, 'exit' to quit{Fore.RESET}"
    )
    print(f"{Fore.YELLOW}{'=' * 70}{Fore.RESET}\n")


class AsyncNetworkScanner:
    def __init__(self):
        self.session = None

    async def create_session(self):
        if not self.session:
            self.session = aiohttp.ClientSession()
        return self.session

    async def close_session(self):
        if self.session:
            await self.session.close()
            self.session = None

    async def scan_port_async(self, host: str, port: int, timeout: float = 2.0) -> Dict:
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port), timeout=timeout
            )
            writer.close()
            await writer.wait_closed()
            return {"port": port, "status": "OPEN", "host": host}
        except:
            return {"port": port, "status": "CLOSED", "host": host}

    async def scan_ports_bulk(
        self, host: str, ports: List[int], max_concurrent: int = 100
    ) -> List[Dict]:
        semaphore = asyncio.Semaphore(max_concurrent)

        async def bounded_scan(port):
            async with semaphore:
                return await self.scan_port_async(host, port)

        tasks = [bounded_scan(port) for port in ports]
        return await asyncio.gather(*tasks)

    async def dns_resolve_async(self, domain: str, record_type: str = "A") -> Dict:
        try:
            resolver = dns.resolver.Resolver()
            resolver.timeout = 5
            answers = await asyncio.get_event_loop().run_in_executor(
                None, resolver.resolve, domain, record_type
            )
            return {
                "domain": domain,
                "record_type": record_type,
                "records": [str(a) for a in answers],
                "success": True,
            }
        except Exception as e:
            return {
                "domain": domain,
                "record_type": record_type,
                "success": False,
                "error": str(e),
            }

    async def reverse_dns_async(self, ip_address: str) -> Dict:
        try:
            result = await asyncio.get_event_loop().run_in_executor(
                None, socket.gethostbyaddr, ip_address
            )
            return {"ip": ip_address, "hostname": result[0], "success": True}
        except Exception as e:
            return {"ip": ip_address, "success": False, "error": str(e)}

    async def zeroconf_discover(
        self, service_type: str = "_http._tcp.local.", timeout: int = 5
    ) -> List[Dict]:
        services = []

        class Listener:
            def __init__(self, results):
                self.results = results

            def add_service(self, zc, type_, name):
                info = zc.get_service_info(type_, name)
                if info:
                    self.results.append(
                        {"name": name, "host": str(info.server), "port": info.port}
                    )

            def remove_service(self, zc, type_, name):
                pass

        listener = Listener(services)
        zc = zeroconf.Zeroconf()
        try:
            browser = zeroconf.ServiceBrowser(zc, service_type, listener)
            await asyncio.sleep(timeout)
            browser.cancel()
        finally:
            zc.close()
        return services

    async def websocket_test(
        self, uri: str, message: str = "ping", timeout: int = 5
    ) -> Dict:
        try:
            async with websockets.connect(uri, timeout=timeout) as websocket:
                await websocket.send(message)
                response = await websocket.recv()
                return {
                    "uri": uri,
                    "connected": True,
                    "response": response,
                    "success": True,
                }
        except Exception as e:
            return {"uri": uri, "connected": False, "error": str(e), "success": False}


class CommandResult:
    def __init__(self, success: bool, output: str, error: str = "", exit_code: int = 0):
        self.success = success
        self.output = output
        self.error = error
        self.exit_code = exit_code


class NetworkToolkit:
    def __init__(self):
        self.os_name = platform.system()
        self.history: List[Dict] = []
        self.async_scanner = AsyncNetworkScanner()
        self.executor = ThreadPoolExecutor(max_workers=10)

    def safe_decode(self, output: bytes) -> str:
        for encoding in ["utf-8", "cp866", "cp1251", "latin-1"]:
            try:
                return output.decode(encoding, errors="strict")
            except UnicodeDecodeError:
                continue
        return output.decode("cp1251", errors="replace")

    def run_command(self, cmd: str) -> CommandResult:
        self.history.append({"timestamp": datetime.now().isoformat(), "command": cmd})
        try:
            result = subprocess.run(
                cmd, shell=True, capture_output=True, timeout=30, encoding=None
            )
            stdout = self.safe_decode(result.stdout) if result.stdout else ""
            stderr = self.safe_decode(result.stderr) if result.stderr else ""
            if not stdout and not stderr and result.returncode == 0:
                stdout = "Command executed successfully"
            return CommandResult(
                result.returncode == 0, stdout, stderr, result.returncode
            )
        except subprocess.TimeoutExpired:
            return CommandResult(False, "", "Timeout (30s)", -2)
        except Exception as e:
            return CommandResult(False, "", str(e), -3)

    def ping(self, host: str, count: int = 4) -> CommandResult:
        cmd = f"ping -n {count} {host}"
        return self.run_command(cmd)

    def tracert(self, host: str) -> CommandResult:
        return self.run_command(f"tracert -d {host}")

    def ipconfig(self, args: str = "") -> CommandResult:
        return self.run_command(f"ipconfig {args}")

    def netstat(self, args: str = "") -> CommandResult:
        return self.run_command(f"netstat {args}")

    def nslookup(self, host: str) -> CommandResult:
        return self.run_command(f"nslookup {host}")

    def arp(self) -> CommandResult:
        return self.run_command("arp -a")

    def route(self) -> CommandResult:
        return self.run_command("route print")

    def getmac(self) -> CommandResult:
        return self.run_command("getmac")

    def hostname(self) -> CommandResult:
        return self.run_command("hostname")

    def whoami(self) -> CommandResult:
        return self.run_command("whoami")

    def systeminfo(self) -> CommandResult:
        return self.run_command(
            'systeminfo | findstr /B /C:"Host Name" /C:"OS Name" /C:"OS Version"'
        )

    def tasklist(self) -> CommandResult:
        return self.run_command("tasklist")

    def driverquery(self) -> CommandResult:
        return self.run_command("driverquery")

    def netsh_wlan(self) -> CommandResult:
        return self.run_command("netsh wlan show profiles")

    def netsh_firewall(self) -> CommandResult:
        return self.run_command("netsh advfirewall show currentprofile")

    def net_share(self) -> CommandResult:
        return self.run_command("net share")

    def net_user(self) -> CommandResult:
        return self.run_command("net user")

    def net_use(self) -> CommandResult:
        return self.run_command("net use")

    def flushdns(self) -> CommandResult:
        return self.run_command("ipconfig /flushdns")

    def get_public_ip(self) -> str:
        try:
            response = requests.get("https://api.ipify.org?format=json", timeout=5)
            return response.json().get("ip", "Unknown")
        except:
            return "Unable to fetch"

    def get_local_ips(self) -> List[str]:
        ips = []
        try:
            hostname = socket.gethostname()
            ips.extend(socket.gethostbyname_ex(hostname)[2])
        except:
            pass
        result = self.run_command("ipconfig")
        if result.success:
            for line in result.output.split("\n"):
                match = re.search(r"IPv4 Address[.\s]*:\s*(\d+\.\d+\.\d+\.\d+)", line)
                if match:
                    ips.append(match.group(1))
        return list(set(ips))

    def sync_port_scan(self, host: str, ports: List[int]) -> Dict[int, str]:
        results = {}
        for port in ports:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(0.5)
                results[port] = (
                    "OPEN" if sock.connect_ex((host, port)) == 0 else "CLOSED"
                )
                sock.close()
            except:
                results[port] = "ERROR"
        return results

    async def async_port_scan(self, host: str, ports: List[int]) -> List[Dict]:
        return await self.async_scanner.scan_ports_bulk(host, ports)

    async def dns_lookup_async(self, domain: str, record_type: str = "A") -> Dict:
        return await self.async_scanner.dns_resolve_async(domain, record_type)

    async def reverse_dns(self, ip: str) -> Dict:
        return await self.async_scanner.reverse_dns_async(ip)

    async def websocket_test(self, uri: str) -> Dict:
        return await self.async_scanner.websocket_test(uri)

    async def zeroconf_scan(self, service: str = "_http._tcp.local.") -> List[Dict]:
        return await self.async_scanner.zeroconf_discover(service)

    def show_history(self, limit: int = 20) -> str:
        if not self.history:
            return "No command history"
        result = []
        for entry in self.history[-limit:]:
            result.append(f"[{entry['timestamp']}] {entry['command']}")
        return "\n".join(result)

    def export_log(self, filename: str = "network_log.txt") -> bool:
        try:
            with open(filename, "w", encoding="utf-8") as f:
                f.write(
                    f"Network Toolkit Log\nGenerated: {datetime.now()}\n{'=' * 60}\n"
                )
                for entry in self.history:
                    f.write(f"{entry['timestamp']} | {entry['command']}\n")
            return True
        except:
            return False

    def clear_history(self):
        self.history = []
        return "History cleared"


def parse_port_range(range_str: str) -> List[int]:
    ports = []
    for part in range_str.split(","):
        if "-" in part:
            start, end = map(int, part.split("-"))
            ports.extend(range(start, end + 1))
        else:
            ports.append(int(part))
    return ports


async def main():
    if os.name != "nt":
        print(f"{Fore.RED}This toolkit is designed for Windows{Fore.RESET}")
        return

    print_banner()
    toolkit = NetworkToolkit()

    commands = {
        "ping": lambda args: toolkit.ping(args[0] if args else "localhost"),
        "tracert": lambda args: toolkit.tracert(args[0] if args else "google.com"),
        "ipconfig": lambda args: toolkit.ipconfig(" ".join(args)),
        "ipconfig /all": lambda args: toolkit.ipconfig("/all"),
        "ipconfig /flushdns": lambda args: toolkit.flushdns(),
        "ipconfig /release": lambda args: toolkit.ipconfig("/release"),
        "ipconfig /renew": lambda args: toolkit.ipconfig("/renew"),
        "ipconfig /displaydns": lambda args: toolkit.ipconfig("/displaydns"),
        "netstat": lambda args: toolkit.netstat(" ".join(args)),
        "netstat -a": lambda args: toolkit.netstat("-a"),
        "netstat -b": lambda args: toolkit.netstat("-b"),
        "netstat -n": lambda args: toolkit.netstat("-n"),
        "netstat -o": lambda args: toolkit.netstat("-o"),
        "nslookup": lambda args: toolkit.nslookup(args[0] if args else "google.com"),
        "arp": lambda args: toolkit.arp(),
        "arp -a": lambda args: toolkit.arp(),
        "route": lambda args: toolkit.route(),
        "route print": lambda args: toolkit.route(),
        "getmac": lambda args: toolkit.getmac(),
        "hostname": lambda args: toolkit.hostname(),
        "whoami": lambda args: toolkit.whoami(),
        "systeminfo": lambda args: toolkit.systeminfo(),
        "tasklist": lambda args: toolkit.tasklist(),
        "driverquery": lambda args: toolkit.driverquery(),
        "netsh wlan": lambda args: toolkit.netsh_wlan(),
        "netsh firewall": lambda args: toolkit.netsh_firewall(),
        "net share": lambda args: toolkit.net_share(),
        "net user": lambda args: toolkit.net_user(),
        "net use": lambda args: toolkit.net_use(),
        "publicip": lambda args: f"Public IP: {toolkit.get_public_ip()}",
        "localips": lambda args: f"Local IPs: {', '.join(toolkit.get_local_ips())}",
        "portscan": lambda args: (
            toolkit.sync_port_scan(args[0], parse_port_range(args[1]))
            if len(args) >= 2
            else "Usage: portscan <host> <ports> (e.g., portscan 192.168.1.1 80,443,8080 or 1-1000)"
        ),
        "asyncscan": lambda args: (
            asyncio.run(toolkit.async_port_scan(args[0], parse_port_range(args[1])))
            if len(args) >= 2
            else "Usage: asyncscan <host> <ports>"
        ),
        "dns": lambda args: (
            asyncio.run(
                toolkit.dns_lookup_async(args[0], args[1] if len(args) > 1 else "A")
            )
            if args
            else "Usage: dns <domain> [record_type]"
        ),
        "reversedns": lambda args: (
            asyncio.run(toolkit.reverse_dns(args[0]))
            if args
            else "Usage: reversedns <ip>"
        ),
        "websocket": lambda args: (
            asyncio.run(toolkit.websocket_test(args[0]))
            if args
            else "Usage: websocket <ws:// or wss:// uri>"
        ),
        "zeroconf": lambda args: asyncio.run(
            toolkit.zeroconf_scan(args[0] if args else "_http._tcp.local.")
        ),
        "history": lambda args: toolkit.show_history(),
        "clear": lambda args: toolkit.clear_history(),
        "export": lambda args: "Exported" if toolkit.export_log() else "Export failed",
        "help": lambda args: show_help(),
        "exit": lambda args: sys.exit(0),
        "quit": lambda args: sys.exit(0),
    }

    def show_help():
        help_text = f"""
{Fore.CYAN}Available Commands:{Fore.RESET}

{Fore.GREEN}Basic Network:{Fore.RESET}
  ping <host>                    - Test connectivity
  tracert <host>                 - Trace route
  nslookup <domain>              - DNS lookup
  arp                            - Show ARP table
  route print                    - Show routing table

{Fore.GREEN}IP Configuration:{Fore.RESET}
  ipconfig                       - Show IP configuration
  ipconfig /all                  - Show detailed IP config
  ipconfig /flushdns             - Clear DNS cache
  ipconfig /release              - Release IP address
  ipconfig /renew                - Renew IP address
  ipconfig /displaydns           - Show DNS cache

{Fore.GREEN}Network Statistics:{Fore.RESET}
  netstat                        - Show network statistics
  netstat -a                     - Show all connections
  netstat -b                     - Show binaries
  netstat -n                     - Show numerical addresses
  netstat -o                     - Show owning process ID

{Fore.GREEN}System Information:{Fore.RESET}
  systeminfo                     - Show system info
  tasklist                       - Show running processes
  driverquery                    - Show drivers
  getmac                         - Show MAC addresses
  hostname                       - Show computer name
  whoami                         - Show current user

{Fore.GREEN}Windows Networking:{Fore.RESET}
  netsh wlan                     - Show WiFi profiles
  netsh firewall                 - Show firewall status
  net share                      - Show shared resources
  net user                       - Show users
  net use                        - Show network drives

{Fore.GREEN}Advanced Tools:{Fore.RESET}
  publicip                       - Get public IP address
  localips                       - Get local IP addresses
  portscan <host> <ports>        - Scan ports (e.g., 80,443 or 1-1000)
  asyncscan <host> <ports>       - Async port scan (faster)
  dns <domain> [type]            - Advanced DNS lookup (A, MX, TXT, NS)
  reversedns <ip>                - Reverse DNS lookup
  websocket <uri>                - Test WebSocket connection
  zeroconf [service]             - Discover ZeroConf services

{Fore.GREEN}Utilities:{Fore.RESET}
  history                        - Show command history
  clear                          - Clear history
  export                         - Export log to file
  help                           - Show this help
  exit                           - Exit program

{Fore.YELLOW}Examples:{Fore.RESET}
  ping google.com
  portscan 192.168.1.1 22,80,443
  dns google.com MX
  websocket ws://echo.websocket.org
{Fore.RESET}"""
        print(help_text)

    print(f"{Fore.GREEN}Type 'help' to see all commands{Fore.RESET}\n")

    while running:
        try:
            user_input = input(f"{Fore.CYAN}ntoolkit>{Fore.RESET} ").strip()
            if not user_input:
                continue

            parts = user_input.split()
            cmd = parts[0].lower()
            args = parts[1:] if len(parts) > 1 else []

            full_cmd = user_input.lower()

            matched = False
            for command in commands:
                if full_cmd.startswith(command):
                    remaining = full_cmd[len(command) :].strip()
                    cmd_args = remaining.split() if remaining else []
                    result = commands[command](cmd_args)
                    matched = True
                    if isinstance(result, CommandResult):
                        print()
                        if result.success:
                            if result.output:
                                print(result.output)
                        else:
                            print(f"{Fore.RED}Error: {result.error}{Fore.RESET}")
                    elif isinstance(result, list):
                        print()
                        for item in result[:30]:
                            if isinstance(item, dict) and item.get("status") == "OPEN":
                                print(
                                    f"{Fore.GREEN}[OPEN] {item['host']}:{item['port']}{Fore.RESET}"
                                )
                            elif isinstance(item, dict):
                                print(f"{Fore.WHITE}{item}{Fore.RESET}")
                            else:
                                print(item)
                        if len(result) > 30:
                            print(
                                f"{Fore.YELLOW}... and {len(result) - 30} more{Fore.RESET}"
                            )
                    elif isinstance(result, dict):
                        print()
                        for k, v in result.items():
                            if "success" in k and v:
                                print(f"{Fore.GREEN}{k}: {v}{Fore.RESET}")
                            elif "error" in k.lower():
                                print(f"{Fore.RED}{k}: {v}{Fore.RESET}")
                            else:
                                print(f"{k}: {v}")
                    elif isinstance(result, str):
                        print(result)
                    break

            if not matched:
                print(f"{Fore.RED}Unknown command: {cmd}{Fore.RESET}")
                print(f"{Fore.YELLOW}Type 'help' for available commands{Fore.RESET}")

        except KeyboardInterrupt:
            print(f"\n{Fore.YELLOW}Shutting down...{Fore.RESET}")
            break
        except Exception as e:
            print(f"{Fore.RED}Error: {e}{Fore.RESET}")


if __name__ == "__main__":
    asyncio.run(main())
